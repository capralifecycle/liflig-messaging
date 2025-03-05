@file:Suppress("unused", "MemberVisibilityCanBePrivate") // This is a library

package no.liflig.messaging.topic

/** Mock implementation of [Topic] for tests and local development. */
public class MockTopic : Topic {
  public val publishedMessages: MutableList<String> = mutableListOf()

  override fun publish(message: String) {
    publishedMessages.add(message)
  }

  /**
   * Gets the latest published message.
   *
   * @throws IllegalStateException If there are no published messages (since we call this in tests
   *   when we expect there to be an outgoing message).
   */
  public fun getPublishedMessage(): String {
    return publishedMessages.lastOrNull()
        ?: throw IllegalStateException(
            "Expected to find published message on topic, but found none",
        )
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
  public fun hasPublished(messageCount: Int): Boolean = publishedMessages.size == messageCount

  public fun clear() {
    publishedMessages.clear()
  }
}
