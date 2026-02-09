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
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.utils.await

/**
 * Mock implementation of [Queue] for tests and local development. There are generally two types of
 * queues that we test with this:
 * - Queues for incoming messages that our application polls with [MessagePoller]. Here you can use
 *   [send] to add a message to be processed by the poller, and then verify the state of the queue
 *   with [awaitProcessed], [expectProcessed], [awaitFailedWithRetry], [expectFailedWithRetry],
 *   [awaitFailedWithoutRetry] and [expectFailedWithoutRetry].
 * - Queues for outgoing messages, which our application sends to with [send]. Here you can use
 *   [awaitSent] / [expectSent].
 *
 * If you reuse the same queue across tests, you should call [clear] between each test.
 */
public class MockQueue : Queue {
  /**
   * Messages sent with [send], that have not been processed yet.
   *
   * Consider using [awaitSent] if you need to wait for some other thread to send to the queue, or
   * [expectSent] if you want to verify the current state of sent messages on the queue.
   *
   * We expose this as a [MutableList] for backwards compatibility, but you probably don't want to
   * use this list directly. In some future major version, we might make this internal.
   */
  public val sentMessages: MutableList<Message> = mutableListOf()

  /**
   * Messages successfully processed by a [MessagePoller] (and its `MessageProcessor`), which then
   * called [delete] on it.
   *
   * Consider using [awaitProcessed] if you need to wait for some other thread to process from the
   * queue, or [expectProcessed] if you want to verify the current state of processed messages on
   * the queue.
   *
   * We expose this as a [MutableList] for backwards compatibility, but you probably don't want to
   * use this list directly. In some future major version, we might make this internal.
   */
  public val processedMessages: MutableList<Message> = mutableListOf()

  /**
   * Messages that failed processing by a [MessagePoller] (and its `MessageProcessor`), which then
   * called [Queue.retry] or [Queue.deleteFailed] on it.
   *
   * Consider using [awaitFailed] if you need to wait for some other thread to process from the
   * queue, or [expectFailed] if you want to verify the current state of failed messages on the
   * queue.
   *
   * We expose this as a [MutableList] for backwards compatibility, but you probably don't want to
   * use this list directly. In some future major version, we might make this internal.
   */
  public val failedMessages: MutableList<Message> = mutableListOf()

  /**
   * Messages that failed processing by a `MessagePoller` (and its `MessageProcessor`), with retry
   * enabled ([Queue.retry] called).
   *
   * Consider using [awaitFailedWithRetry] if you need to wait for some other thread to process from
   * the queue, or [expectFailedWithRetry] if you want to verify the current state of failed
   * messages on the queue.
   *
   * We expose this as a [MutableList] for backwards compatibility, but you probably don't want to
   * use this list directly. In some future major version, we might make this internal.
   */
  public val failedMessagesWithRetry: MutableList<Message> = mutableListOf()

  /**
   * Messages that failed processing by a `MessagePoller` (and its `MessageProcessor`), with retry
   * disabled ([Queue.deleteFailed] called).
   *
   * Consider using [awaitFailedWithoutRetry] if you need to wait for some other thread to process
   * from the queue, or [expectFailedWithoutRetry] if you want to verify the current state of failed
   * messages on the queue.
   *
   * We expose this as a [MutableList] for backwards compatibility, but you probably don't want to
   * use this list directly. In some future major version, we might make this internal.
   */
  public val failedMessagesWithoutRetry: MutableList<Message> = mutableListOf()

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
        failedMessagesWithoutRetry.add(message)

        cond.signalAll()
      }
    }
  }

  override fun retry(message: Message) {
    lock.withLock {
      /** Same logic as [delete]. */
      val removed = sentMessages.remove(message)
      if (removed) {
        failedMessages.add(message)
        failedMessagesWithRetry.add(message)

        cond.signalAll()
      }
    }
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
    return await(lock, cond, timeout) {
      takeMessages(
          messageCount,
          this.failedMessages,
          also = {
            this.failedMessagesWithRetry.clear()
            this.failedMessagesWithoutRetry.clear()
          },
      )
    }
  }

  /**
   * Waits for the given number of messages to fail processing with retry enabled (passed to
   * [Queue.retry]), returns a copy of them, and then clears the [failedMessagesWithRetry] list
   * (also removes these messages from [failedMessages]).
   *
   * If the given [timeout] expires before the messages fail, then a `TimeoutException` is thrown
   * (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * If you want to assert that the queue has the given number of failed messages _right now_,
   * without waiting, then you should call [expectFailed] instead.
   *
   * Example:
   * ```
   * val (failedEvent) = queue.awaitFailedWithRetry(1)
   * ```
   */
  public fun awaitFailedWithRetry(
      messageCount: Int,
      timeout: Duration? = DEFAULT_TIMEOUT,
  ): List<Message> {
    return await(lock, cond, timeout) {
      takeMessages(
          messageCount,
          this.failedMessagesWithRetry,
          also = { this.failedMessages.removeIf { this.failedMessagesWithRetry.contains(it) } },
      )
    }
  }

  /**
   * Waits for the given number of messages to fail processing with retry disabled (passed to
   * [Queue.deleteFailed]), returns a copy of them, and then clears the [failedMessagesWithoutRetry]
   * list (also removes these messages from [failedMessages]).
   *
   * If the given [timeout] expires before the messages fail, then a `TimeoutException` is thrown
   * (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * If you want to assert that the queue has the given number of failed messages _right now_,
   * without waiting, then you should call [expectFailed] instead.
   *
   * Example:
   * ```
   * val (failedEvent) = queue.awaitFailedWithoutRetry(1)
   * ```
   */
  public fun awaitFailedWithoutRetry(
      messageCount: Int,
      timeout: Duration? = DEFAULT_TIMEOUT,
  ): List<Message> {
    return await(lock, cond, timeout) {
      takeMessages(
          messageCount,
          this.failedMessagesWithoutRetry,
          also = { this.failedMessages.removeIf { this.failedMessagesWithoutRetry.contains(it) } },
      )
    }
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
              buildMessageExceptionString(
                  messageCount,
                  this.processedMessages,
                  messageType = "processed messages",
              )
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
              buildMessageExceptionString(
                  messageCount,
                  this.sentMessages,
                  messageType = "sent messages",
              )
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
      return takeMessages(
          messageCount,
          this.failedMessages,
          also = {
            this.failedMessagesWithRetry.clear()
            this.failedMessagesWithoutRetry.clear()
          },
      )
          ?: throw IllegalStateException(
              buildMessageExceptionString(
                  messageCount,
                  this.failedMessages,
                  messageType = "failed messages",
              )
          )
    }
  }

  /**
   * Checks if the queue has the given number of failed messages with retry enabled (passed to
   * [Queue.retry]).
   * - If it does: Returns a copy of the failed messages, and then clears the
   *   [failedMessagesWithRetry] list (also removes these messages from [failedMessages])
   * - If it does not: Throws an [IllegalStateException]
   *
   * If you want to wait until some other thread fails processing of the given number of messages,
   * call [awaitFailedWithRetry] instead.
   *
   * Example:
   * ```
   * val (event) = queue.expectFailedWithRetry(1)
   * ```
   */
  public fun expectFailedWithRetry(messageCount: Int): List<Message> {
    lock.withLock {
      return takeMessages(
          messageCount,
          this.failedMessagesWithRetry,
          also = { this.failedMessages.removeIf { this.failedMessagesWithRetry.contains(it) } },
      )
          ?: throw IllegalStateException(
              buildMessageExceptionString(
                  messageCount,
                  this.failedMessagesWithRetry,
                  messageType = "failed messages (with retry enabled)",
              )
          )
    }
  }

  /**
   * Checks if the queue has the given number of failed messages with retry disabled (passed to
   * [Queue.deleteFailed]).
   * - If it does: Returns a copy of the failed messages, and then clears the
   *   [failedMessagesWithoutRetry] list (also removes these messages from [failedMessages])
   * - If it does not: Throws an [IllegalStateException]
   *
   * If you want to wait until some other thread fails processing of the given number of messages,
   * call [awaitFailedWithoutRetry] instead.
   *
   * Example:
   * ```
   * val (event) = queue.expectFailedWithoutRetry(1)
   * ```
   */
  public fun expectFailedWithoutRetry(messageCount: Int): List<Message> {
    lock.withLock {
      return takeMessages(
          messageCount,
          this.failedMessagesWithoutRetry,
          also = { this.failedMessages.removeIf { this.failedMessagesWithoutRetry.contains(it) } },
      )
          ?: throw IllegalStateException(
              buildMessageExceptionString(
                  messageCount,
                  this.failedMessagesWithoutRetry,
                  messageType = "failed messages (with retry disabled)",
              )
          )
    }
  }

  /**
   * Returns true if the queue has the given number of successfully processed messages (see
   * [processedMessages]).
   *
   * Consider using [awaitProcessed] / [expectProcessed] instead.
   */
  public fun hasProcessed(messageCount: Int): Boolean {
    lock.withLock {
      return processedMessages.size == messageCount
    }
  }

  /**
   * Returns true if the given number of messages has been sent to the queue (see [sentMessages]).
   *
   * Consider using [awaitSent] / [expectSent] instead.
   */
  public fun hasSent(messageCount: Int): Boolean {
    lock.withLock {
      return sentMessages.size == messageCount
    }
  }

  /**
   * Returns true if the queue has the given number of failed messages (see [failedMessages]).
   *
   * Consider using [awaitFailed] / [expectFailed] instead.
   */
  public fun hasFailed(messageCount: Int): Boolean {
    lock.withLock {
      return failedMessages.size == messageCount
    }
  }

  /**
   * Returns true if the queue has the given number of failed messages with retry enabled.
   *
   * Consider using [awaitFailedWithRetry] / [expectFailedWithRetry] instead.
   */
  public fun hasFailedWithRetry(messageCount: Int): Boolean {
    lock.withLock {
      return failedMessagesWithRetry.size == messageCount
    }
  }

  /**
   * Returns true if the queue has the given number of failed messages with retry disabled.
   *
   * Consider using [awaitFailedWithoutRetry] / [expectFailedWithoutRetry] instead.
   */
  public fun hasFailedWithoutRetry(messageCount: Int): Boolean {
    lock.withLock {
      return failedMessagesWithoutRetry.size == messageCount
    }
  }

  /**
   * Gets the latest outgoing message from [send].
   *
   * Consider using [awaitSent] / [expectSent] instead.
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
   * Consider using [awaitFailed] / [expectFailed] instead.
   *
   * @throws IllegalStateException If there are no failed messages (since we call this in tests when
   *   we expect there to be a failed message).
   */
  public fun getFailedMessage(): Message {
    lock.withLock {
      return failedMessages.lastOrNull()
          ?: throw IllegalStateException("Expected to find failed message on queue, but found none")
    }
  }

  /**
   * Gets the latest failed message from [Queue.retry].
   *
   * Consider using [awaitFailedWithRetry] / [expectFailedWithRetry] instead.
   *
   * @throws IllegalStateException If there are no failed messages with retry enabled (since we call
   *   this in tests when we expect there to be a failed message).
   */
  public fun getFailedMessageWithRetry(): Message {
    lock.withLock {
      return failedMessagesWithRetry.lastOrNull()
          ?: throw IllegalStateException(
              "Expected to find failed message (with retry enabled) on queue, but found none"
          )
    }
  }

  /**
   * Gets the latest failed message from [Queue.deleteFailed].
   *
   * Consider using [awaitFailedWithoutRetry] / [expectFailedWithoutRetry] instead.
   *
   * @throws IllegalStateException If there are no failed messages with retry disabled (since we
   *   call this in tests when we expect there to be a failed message).
   */
  public fun getFailedMessageWithoutRetry(): Message {
    lock.withLock {
      return failedMessagesWithoutRetry.lastOrNull()
          ?: throw IllegalStateException(
              "Expected to find failed message (with retry disabled) on queue, but found none"
          )
    }
  }

  /**
   * Clears sent, processed and failed messages on the queue. Call this between tests to reset
   * state.
   */
  public fun clear() {
    lock.withLock {
      sentMessages.clear()
      processedMessages.clear()
      failedMessages.clear()
      failedMessagesWithRetry.clear()
      failedMessagesWithoutRetry.clear()
    }
  }

  private companion object {
    private val DEFAULT_TIMEOUT = Duration.ofSeconds(10)

    /**
     * Checks if the given [messages] list has size equal to the given [expectedMessageCount].
     * - If true: Returns a copy of the given messages, clears the message list, and invokes the
     *   given [also] function (if given)
     * - If false: Returns `null`
     *
     * [MockQueue.lock] must be held when this is called on one of its message lists. This is the
     * case in all the `await`/`expect` methods where we call this.
     */
    private fun takeMessages(
        expectedMessageCount: Int,
        messages: MutableList<Message>,
        also: (() -> Unit)? = null,
    ): List<Message>? {
      if (messages.size == expectedMessageCount) {
        also?.invoke()

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
        append(" on queue, got ")
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
