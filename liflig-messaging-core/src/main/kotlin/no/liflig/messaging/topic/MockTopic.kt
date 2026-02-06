@file:Suppress("unused", "MemberVisibilityCanBePrivate") // This is a library

package no.liflig.messaging.topic

import java.time.Duration
import java.util.UUID
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import no.liflig.messaging.MessageId
import no.liflig.messaging.utils.await

/** Mock implementation of [Topic] for tests and local development. */
public class MockTopic : Topic {
  public val publishedMessages: MutableList<String> = mutableListOf()
  /** Read/write lock, to synchronize reads and writes to the message list. */
  internal val lock: Lock = ReentrantLock()
  /** Condition variable, to notify waiters when a message is published to the topic. */
  private val cond = lock.newCondition()

  override fun publish(message: String): MessageId {
    lock.withLock {
      publishedMessages.add(message)

      cond.signalAll()

      return MessageId(UUID.randomUUID().toString()) // Random mock message ID
    }
  }

  /**
   * Gets the latest published message.
   *
   * @throws IllegalStateException If there are no published messages (since we call this in tests
   *   when we expect there to be an outgoing message).
   */
  public fun getPublishedMessage(): String {
    lock.withLock {
      return publishedMessages.lastOrNull()
          ?: throw IllegalStateException(
              "Expected to find published message on topic, but found none"
          )
    }
  }

  /**
   * Can be used together with awaitility in tests, to wait until the given number of messages has
   * been published to the topic. Example:
   * ```
   * import org.awaitility.kotlin.await
   *
   * await.until { topic.hasPublished(1) }
   * ```
   */
  public fun hasPublished(messageCount: Int): Boolean {
    lock.withLock {
      return publishedMessages.size == messageCount
    }
  }

  /**
   * Waits for the given number of messages to be published (passed to [Topic.publish]), returns
   * them, and then clears the [publishedMessages] list.
   *
   * If the given [timeout] expires before the messages are published, then a `TimeoutException` is
   * thrown (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * Example:
   * ```
   * val (event) = topic.awaitPublished(1)
   * ```
   */
  public fun awaitPublished(messageCount: Int, timeout: Duration? = DEFAULT_TIMEOUT): List<String> {
    return await(lock, cond, timeout) {
      if (publishedMessages.size == messageCount) {
        val copy = ArrayList(publishedMessages)
        publishedMessages.clear()
        copy
      } else {
        null
      }
    }
  }

  public fun clear() {
    lock.withLock { publishedMessages.clear() }
  }

  private companion object {
    private val DEFAULT_TIMEOUT = Duration.ofSeconds(10)
  }
}
