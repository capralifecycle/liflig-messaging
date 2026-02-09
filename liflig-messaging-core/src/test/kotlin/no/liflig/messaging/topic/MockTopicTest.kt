package no.liflig.messaging.topic

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlin.concurrent.thread
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

internal class MockTopicTest {
  val topic = MockTopic()

  @BeforeEach
  fun reset() {
    topic.clear()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitPublished`() {
    val messages = listOf("1", "2")

    var result: List<String>? = null
    val consumer = thread { result = topic.awaitPublished(messages.size) }

    val producer = thread {
      messages.forEach {
        topic.publish(it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    topic.publishedMessages.shouldBeEmpty()
  }

  @Test
  fun `test successful expectPublished`() {
    val messages = listOf("msg1", "msg2")
    topic.publishedMessages.addAll(messages)

    val actualMessages = topic.expectPublished(2)
    actualMessages.shouldBe(messages)
    topic.publishedMessages.shouldBeEmpty()
  }

  @Test
  fun `test expectPublished exception`() {
    val messages = listOf("msg1", "msg2")
    topic.publishedMessages.addAll(messages)

    val exception = shouldThrow<IllegalStateException> { topic.expectPublished(3) }
    exception.message.shouldBe(
        """
        Expected 3 published messages on topic, got 2:
        	1: msg1
        	2: msg2
        """
            .trimIndent()
    )
  }
}
