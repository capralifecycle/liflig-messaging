@file:Suppress("unused", "MemberVisibilityCanBePrivate") // This is a library

package no.liflig.messaging.topic

import java.time.Duration
import java.util.UUID
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import no.liflig.messaging.MessageId
import no.liflig.messaging.utils.await

/**
 * Mock implementation of [Topic] for tests and local development. Provides [awaitPublished] and
 * [expectPublished] for testing the state of messages on the topic.
 *
 * If you reuse the same topic across tests, you should call [clear] between each test.
 */
public class MockTopic : Topic {
  /**
   * Messages published to the topic with [publish].
   *
   * Consider using [awaitPublished] if you need to wait for some other thread to publish to the
   * topic, or [expectPublished] if you want to verify the current state of published messages on
   * the topic.
   *
   * We expose this as a [MutableList] for backwards compatibility, but you probably don't want to
   * use this list directly. In some future major version, we might make this internal.
   */
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
   * Waits for the given number of messages to be published (passed to [Topic.publish]), returns
   * them, and then clears the [publishedMessages] list.
   *
   * If the given [timeout] expires before the messages are published, then a `TimeoutException` is
   * thrown (default timeout is 10 seconds, set to `null` to wait forever).
   *
   * If you want to assert that the topic has the given number of published messages _right now_,
   * without waiting, then you should call [expectPublished] instead.
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

  /**
   * Checks if the topic has the given number of published messages (passed to [Topic.publish]).
   * - If it does: Returns a copy of the published messages, and then clears the [publishedMessages]
   *   list
   * - If it does not: Throws an [IllegalStateException]
   *
   * If you want to wait until some other thread publishes the given number of messages, call
   * [awaitPublished] instead.
   *
   * Example:
   * ```
   * val (event) = topic.expectPublished(1)
   * ```
   */
  public fun expectPublished(messageCount: Int): List<String> {
    lock.withLock {
      return takeMessages(messageCount)
          ?: throw IllegalStateException(buildMessageExceptionString(messageCount))
    }
  }

  /**
   * Gets the latest published message.
   *
   * Consider using [awaitPublished] / [expectPublished] instead.
   *
   * @throws IllegalStateException If there are no published messages (since we call this in tests
   *   when we expect there to be a published message).
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
   * Returns true if the topic has the given number of published messages (see [publishedMessages]).
   *
   * Consider using [awaitPublished] / [expectPublished] instead.
   */
  public fun hasPublished(messageCount: Int): Boolean {
    lock.withLock {
      return publishedMessages.size == messageCount
    }
  }

  /**
   * Clears sent, processed and failed messages on the queue. Call this between tests to reset
   * state.
   */
  public fun clear() {
    lock.withLock { publishedMessages.clear() }
  }

  /**
   * Checks if the [publishedMessages] list has size equal to the given [expectedMessageCount].
   * - If true: Returns a copy of the published messages, and clears the [publishedMessages] list
   * - If false: Returns `null`
   *
   * [MockTopic.lock] must be held when this is called. This is the case in all the `await`/`expect`
   * methods where we call this.
   */
  private fun takeMessages(expectedMessageCount: Int): List<String>? {
    if (publishedMessages.size == expectedMessageCount) {
      val copy = ArrayList(publishedMessages)
      publishedMessages.clear()
      return copy
    } else {
      return null
    }
  }

  private fun buildMessageExceptionString(expectedMessageCount: Int): String {
    return buildString {
      append("Expected ")
      append(expectedMessageCount)
      append(" published messages on topic, got ")
      append(publishedMessages.size)

      // If there were more than 0 messages on the topic, include the messages in the exception
      // for debugging
      if (publishedMessages.isNotEmpty()) {
        append(":")

        publishedMessages.forEachIndexed { index, message ->
          append("\n\t")
          // If there's more than 1 message, we use a numbered list to separate them
          if (publishedMessages.size != 1) {
            append(index + 1)
            append(": ")
          }
          append(message)
        }
      }
    }
  }

  private companion object {
    private val DEFAULT_TIMEOUT = Duration.ofSeconds(10)
  }
}
