@file:Suppress("unused", "MemberVisibilityCanBePrivate") // This is a library

package no.liflig.messaging.queue

import java.time.Duration
import java.util.UUID
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import no.liflig.messaging.Message
import no.liflig.messaging.MessageId
import no.liflig.messaging.utils.await

/**
 * Mock implementation of [Queue] for tests and local development. There are generally two types of
 * queues that we test with this:
 * - Queues for incoming messages that our application polls with MessagePoller. Here you can use
 *   [send] to add a message to be processed by the poller, and then check [processedMessages] or
 *   [hasProcessed] to verify that it was processed.
 * - Queues for outgoing messages, which our application sends to with [send]. Here you can verify
 *   that an expected message was sent with [getSentMessage].
 */
public class MockQueue : Queue {
  /**
   * Messages sent with [send], that have not been processed yet.
   *
   * Consider using [awaitSent] if you need to wait for some other thread to send to the queue.
   */
  public val sentMessages: MutableList<Message> = mutableListOf()

  /** See [sentMessages]. */
  public val sentMessageCount: Int
    get() = lock.withLock { sentMessages.size }

  /**
   * Messages successfully processed by a `MessagePoller` (and its `MessageProcessor`), which then
   * called [delete] on it.
   *
   * Consider using [awaitProcessed] if you need to wait for some other thread to process from the
   * queue.
   */
  public val processedMessages: MutableList<Message> = mutableListOf()

  /** See [processedMessages]. */
  public val processedMessageCount: Int
    get() = lock.withLock { processedMessages.size }

  /**
   * Messages that failed processing by a `MessagePoller` (and its `MessageProcessor`), which then
   * called [Queue.retry] or [Queue.deleteFailed] on it.
   *
   * Consider using [awaitFailed] if you need to wait for some other thread to process from the
   * queue.
   */
  public val failedMessages: MutableList<Message> = mutableListOf()

  /** See [failedMessages]. */
  public val failedMessageCount: Int
    get() = lock.withLock { failedMessages.size }

  /** Read/write lock, to synchronize reads and writes to the different message lists. */
  internal val lock: Lock = ReentrantLock()
  /**
   * We want to avoid busy-waiting on the [lock] in [poll], as that may slow down our tests. So
   * instead, we use a condition variable, and wait on it in [poll] if no messages have been sent
   * yet. Then, we call [Condition.signalAll] in [send], which wakes the poller.
   *
   * We also use this in our test utility methods [awaitProcessed], [awaitFailed] and [awaitSent].
   *
   * See the docstring on the [await] utility function for how we wait on this condition variable.
   */
  private val cond = lock.newCondition()

  override fun send(
      messageBody: String,
      customAttributes: Map<String, String>,
      systemAttributes: Map<String, String>,
      delay: Duration?,
  ): MessageId {
    lock.withLock {
      val message =
          Message(
              id = MessageId(UUID.randomUUID().toString()),
              body = messageBody,
              systemAttributes = systemAttributes,
              customAttributes = customAttributes,
              receiptHandle = UUID.randomUUID().toString(),
          )
      sentMessages.add(message)

      cond.signalAll()

      return message.id
    }
  }

  override fun poll(): List<Message> {
    /** See [cond]. */
    return await(lock, cond, timeout = null) {
      if (sentMessages.isNotEmpty()) {
        // Copy list, for thread safety
        ArrayList(sentMessages)
      } else {
        null
      }
    }
  }

  override fun delete(message: Message) {
    lock.withLock {
      /**
       * We may get a race condition when calling [clear], in the following case:
       * - MessagePoller calls poll(), getting a copy of [sentMessages]
       * - clear is called, clearing the [sentMessages] list
       * - MessagePoller calls delete() on a message, which adds it to [processedMessages] - which
       *   we don't want, since we wanted to clear the queue
       *
       * To avoid this, we check here if the message actually was in [sentMessages] (i.e., not
       * cleared) before adding it to [processedMessages].
       */
      val removed = sentMessages.remove(message)
      if (removed) {
        processedMessages.add(message)

        cond.signalAll()
      }
    }
  }

  override fun deleteFailed(message: Message) {
    lock.withLock {
      /** Same logic as [delete]. */
      val removed = sentMessages.remove(message)
      if (removed) {
        failedMessages.add(message)

        cond.signalAll()
      }
    }
  }

  override fun retry(message: Message) {
    /**
     * At the moment, `MockQueue` does not separate between messages that failed with/without retry.
     * If there is need for that in the future, then we should maintain 2 lists of failed messages,
     * one for messages that failed with retry and one for messages that failed without retry.
     */
    this.deleteFailed(message)
  }

  /**
   * Waits for the given number of messages to be successfully processed (passed to [Queue.delete]),
   * returns them, and then clears the [processedMessages] list.
   *
   * If the given [timeout] expires before the messages are processed, then a `TimeoutException` is
   * thrown (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * Example:
   * ```
   * val (event) = queue.awaitProcessed(1)
   * ```
   */
  public fun awaitProcessed(
      messageCount: Int,
      timeout: Duration? = DEFAULT_TIMEOUT,
  ): List<Message> {
    return await(lock, cond, timeout) {
      if (processedMessages.size == messageCount) {
        val copy = ArrayList(processedMessages)
        processedMessages.clear()
        copy
      } else {
        null
      }
    }
  }

  /**
   * Waits for the given number of messages to be sent to the queue (passed to [Queue.send]),
   * returns them, and then clears the [sentMessages] list.
   *
   * If the given [timeout] expires before the messages are sent, then a `TimeoutException` is
   * thrown (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * Example:
   * ```
   * val (event) = queue.awaitSent(1)
   * ```
   */
  public fun awaitSent(messageCount: Int, timeout: Duration? = DEFAULT_TIMEOUT): List<Message> {
    return await(lock, cond, timeout) {
      if (sentMessages.size == messageCount) {
        val copy = ArrayList(sentMessages)
        sentMessages.clear()
        copy
      } else {
        null
      }
    }
  }

  /**
   * Waits for the given number of messages to fail processing (passed to [Queue.retry] or
   * [Queue.deleteFailed]), returns them, and then clears the [failedMessages] list.
   *
   * If the given [timeout] expires before the messages fail, then a `TimeoutException` is thrown
   * (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * Example:
   * ```
   * val (failedEvent) = queue.awaitFailed(1)
   * ```
   */
  public fun awaitFailed(messageCount: Int, timeout: Duration? = DEFAULT_TIMEOUT): List<Message> {
    return await(lock, cond, timeout) {
      if (failedMessages.size == messageCount) {
        val copy = ArrayList(failedMessages)
        failedMessages.clear()
        copy
      } else {
        null
      }
    }
  }

  /**
   * Returns true if the queue has the given number of successfully processed messages (see
   * [processedMessages]).
   *
   * Consider using [awaitProcessed] instead.
   */
  public fun hasProcessed(messageCount: Int): Boolean {
    lock.withLock {
      return processedMessages.size == messageCount
    }
  }

  /**
   * Returns true if the given number of messages has been sent to the queue (see [sentMessages]).
   *
   * Consider using [awaitSent] instead.
   */
  public fun hasSent(messageCount: Int): Boolean {
    lock.withLock {
      return sentMessages.size == messageCount
    }
  }

  /** Returns true if the queue has the given number of failed messages (see [failedMessages]). */
  public fun hasFailed(messageCount: Int): Boolean {
    lock.withLock {
      return failedMessages.size == messageCount
    }
  }

  /**
   * Gets the latest outgoing message from [send].
   *
   * Consider using [awaitSent] instead.
   *
   * @throws IllegalStateException If there are no outgoing messages (since we call this in tests
   *   when we expect there to be an outgoing message).
   */
  public fun getSentMessage(): Message {
    lock.withLock {
      return sentMessages.lastOrNull()
          ?: throw IllegalStateException("Expected to find sent message on queue, but found none")
    }
  }

  /**
   * Gets the latest failed message from [Queue.retry] / [Queue.deleteFailed].
   *
   * Consider using [awaitFailed] instead.
   *
   * @throws IllegalStateException If there are no outgoing messages (since we call this in tests
   *   when we expect there to be an outgoing message).
   */
  public fun getFailedMessage(): Message {
    lock.withLock {
      return failedMessages.lastOrNull()
          ?: throw IllegalStateException("Expected to find failed message on queue, but found none")
    }
  }

  public fun clear() {
    lock.withLock {
      sentMessages.clear()
      processedMessages.clear()
      failedMessages.clear()
    }
  }

  private companion object {
    private val DEFAULT_TIMEOUT = Duration.ofSeconds(10)
  }
}
