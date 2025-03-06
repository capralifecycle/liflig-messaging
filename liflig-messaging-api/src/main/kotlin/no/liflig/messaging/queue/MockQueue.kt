@file:Suppress("unused", "MemberVisibilityCanBePrivate") // This is a library

package no.liflig.messaging.queue

import java.time.Duration
import java.util.UUID
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import no.liflig.messaging.Message

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
  /** Messages sent with [send], that have not been processed yet. */
  public val sentMessages: MutableList<Message> = mutableListOf()
  /**
   * Messages processed by a `MessagePoller` (and its `MessageProcessor`), which then called
   * [delete] on it. A message added here typically means it was successfully processed, though it
   * may also mean that it failed with retry disabled.
   */
  public val processedMessages: MutableList<Message> = mutableListOf()
  /**
   * Messages processed by a `MessagePoller` (and its `MessageProcessor`), which then called [retry]
   * on it to retry the message, due to a failure in processing it.
   */
  public val failedMessages: MutableList<Message> = mutableListOf()

  /** Read/write lock, to synchronize reads and writes to the different message lists. */
  private val lock = ReentrantReadWriteLock()

  override fun send(
      messageBody: String,
      customAttributes: Map<String, String>,
      systemAttributes: Map<String, String>,
      delay: Duration?
  ) {
    lock.write {
      sentMessages.add(
          Message(
              id = UUID.randomUUID().toString(),
              body = messageBody,
              systemAttributes = systemAttributes,
              customAttributes = customAttributes,
              receiptHandle = UUID.randomUUID().toString(),
          ),
      )
    }
  }

  override fun poll(): List<Message> {
    lock.read {
      // Copy the list, so the list is not modified by a different thread concurrently
      return ArrayList(sentMessages)
    }
  }

  override fun delete(message: Message) {
    lock.write {
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
      }
    }
  }

  override fun retry(message: Message) {
    lock.write {
      /** Same logic as [delete]. */
      val removed = sentMessages.remove(message)
      if (removed) {
        failedMessages.add(message)
      }
    }
  }

  /**
   * Can be used together with awaitility in tests, to wait until the queue has processed the given
   * number of messages. Example:
   * ```
   * import org.awaitility.kotlin.await
   *
   * await.until { queue.hasProcessed(1) }
   * ```
   */
  public fun hasProcessed(messageCount: Int): Boolean {
    lock.write {
      return processedMessages.size == messageCount
    }
  }

  /** Checks if the queue has the given count of outgoing messages (from [send]). */
  public fun hasSent(messageCount: Int): Boolean {
    lock.read {
      return sentMessages.size == messageCount
    }
  }

  /**
   * Gets the latest outgoing message from [send].
   *
   * @throws IllegalStateException If there are no outgoing messages (since we call this in tests
   *   when we expect there to be an outgoing message).
   */
  public fun getSentMessage(): Message {
    lock.read {
      return sentMessages.lastOrNull()
          ?: throw IllegalStateException("Expected to find sent message on queue, but found none")
    }
  }

  /** Checks if the queue has the given count of failed messages (see [failedMessages]). */
  public fun hasFailed(messageCount: Int): Boolean {
    lock.read {
      return failedMessages.size == messageCount
    }
  }

  /**
   * Gets the latest failed message from [retry].
   *
   * @throws IllegalStateException If there are no outgoing messages (since we call this in tests
   *   when we expect there to be an outgoing message).
   */
  public fun getFailedMessage(): Message {
    lock.read {
      return failedMessages.lastOrNull()
          ?: throw IllegalStateException("Expected to find failed message on queue, but found none")
    }
  }

  public fun clear() {
    lock.write {
      sentMessages.clear()
      processedMessages.clear()
      failedMessages.clear()
    }
  }
}
