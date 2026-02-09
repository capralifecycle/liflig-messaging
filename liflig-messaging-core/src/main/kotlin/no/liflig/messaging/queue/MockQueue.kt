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

  /**
   * Messages successfully processed by a `MessagePoller` (and its `MessageProcessor`), which then
   * called [delete] on it.
   *
   * Consider using [awaitProcessed] if you need to wait for some other thread to process from the
   * queue.
   */
  public val processedMessages: MutableList<Message> = mutableListOf()

  /**
   * Messages that failed processing by a `MessagePoller` (and its `MessageProcessor`), which then
   * called [Queue.retry] or [Queue.deleteFailed] on it.
   *
   * Consider using [awaitFailed] if you need to wait for some other thread to process from the
   * queue.
   */
  public val failedMessages: MutableList<Message> = mutableListOf()

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
   * returns a copy of them, and then clears the [processedMessages] list.
   *
   * If the given [timeout] expires before the messages are processed, then a `TimeoutException` is
   * thrown (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * If you want to assert that the queue has the given number of processed messages _right now_,
   * without waiting, then you should call [expectProcessed] instead.
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
    return await(lock, cond, timeout) { takeMessages(messageCount, this.processedMessages) }
  }

  /**
   * Waits for the given number of messages to be sent to the queue (passed to [Queue.send]),
   * returns a copy of them, and then clears the [sentMessages] list.
   *
   * If the given [timeout] expires before the messages are sent, then a `TimeoutException` is
   * thrown (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * If you want to assert that the queue has the given number of sent messages _right now_, without
   * waiting, then you should call [expectSent] instead.
   *
   * Example:
   * ```
   * val (event) = queue.awaitSent(1)
   * ```
   */
  public fun awaitSent(messageCount: Int, timeout: Duration? = DEFAULT_TIMEOUT): List<Message> {
    return await(lock, cond, timeout) { takeMessages(messageCount, this.sentMessages) }
  }

  /**
   * Waits for the given number of messages to fail processing (passed to [Queue.retry] or
   * [Queue.deleteFailed]), returns a copy of them, and then clears the [failedMessages] list.
   *
   * If the given [timeout] expires before the messages fail, then a `TimeoutException` is thrown
   * (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * If you want to assert that the queue has the given number of failed messages _right now_,
   * without waiting, then you should call [expectFailed] instead.
   *
   * Example:
   * ```
   * val (failedEvent) = queue.awaitFailed(1)
   * ```
   */
  public fun awaitFailed(messageCount: Int, timeout: Duration? = DEFAULT_TIMEOUT): List<Message> {
    return await(lock, cond, timeout) { takeMessages(messageCount, this.failedMessages) }
  }

  /**
   * Checks if the queue has the given number of successfully processed messages (passed to
   * [Queue.delete]).
   * - If it does: Returns a copy of the processed messages, and then clears the [processedMessages]
   *   list
   * - If it does not: Throws an [IllegalStateException]
   *
   * If you want to wait until some other thread processes the given number of messages, call
   * [awaitProcessed] instead.
   *
   * Example:
   * ```
   * val (event) = queue.expectProcessed(1)
   * ```
   */
  public fun expectProcessed(messageCount: Int): List<Message> {
    lock.withLock {
      return takeMessages(messageCount, this.processedMessages)
          ?: throw IllegalStateException(
              buildMessageExceptionString(messageCount, this.processedMessages, "processed")
          )
    }
  }

  /**
   * Checks if the queue has the given number of sent messages (passed to [Queue.send]).
   * - If it does: Returns a copy of the sent messages, and then clears the [sentMessages] list
   * - If it does not: Throws an [IllegalStateException]
   *
   * If you want to wait until some other thread sends the given number of messages, call
   * [awaitSent] instead.
   *
   * Example:
   * ```
   * val (event) = queue.expectSent(1)
   * ```
   */
  public fun expectSent(messageCount: Int): List<Message> {
    lock.withLock {
      return takeMessages(messageCount, this.sentMessages)
          ?: throw IllegalStateException(
              buildMessageExceptionString(messageCount, this.sentMessages, "sent")
          )
    }
  }

  /**
   * Checks if the queue has the given number of failed messages (passed to [Queue.retry] or
   * [Queue.deleteFailed]).
   * - If it does: Returns a copy of the failed messages, and then clears the [failedMessages] list
   * - If it does not: Throws an [IllegalStateException]
   *
   * If you want to wait until some other thread fails processing of the given number of messages,
   * call [awaitFailed] instead.
   *
   * Example:
   * ```
   * val (event) = queue.expectFailed(1)
   * ```
   */
  public fun expectFailed(messageCount: Int): List<Message> {
    lock.withLock {
      return takeMessages(messageCount, this.failedMessages)
          ?: throw IllegalStateException(
              buildMessageExceptionString(messageCount, this.failedMessages, "failed")
          )
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

    /**
     * Checks if the given [messages] list has size equal to the given [expectedMessageCount].
     * - If true: Returns a copy of the given messages, and clear the old messages list
     * - If false: Returns `null`
     *
     * [MockQueue.lock] must be held when this is called on one of its message lists. This is the
     * case in all the `await`/`expect` methods where we call this.
     */
    private fun takeMessages(
        expectedMessageCount: Int,
        messages: MutableList<Message>,
    ): List<Message>? {
      if (messages.size == expectedMessageCount) {
        val copy = ArrayList(messages)
        messages.clear()
        return copy
      } else {
        return null
      }
    }

    private fun buildMessageExceptionString(
        expectedMessageCount: Int,
        messages: List<Message>,
        messageType: String,
    ): String {
      return buildString {
        append("Expected ")
        append(expectedMessageCount)
        append(" ")
        append(messageType)
        append(" messages on queue, got ")
        append(messages.size)

        // If there were more than 0 messages in the queue, include the messages in the exception
        // for debugging
        if (messages.isNotEmpty()) {
          append(":")

          messages.forEachIndexed { index, message ->
            append("\n\t")
            // If there's more than 1 message, we use a numbered list to separate them
            if (messages.size != 1) {
              append(index + 1)
              append(": ")
            }
            append(message.body)
          }
        }
      }
    }
  }
}
