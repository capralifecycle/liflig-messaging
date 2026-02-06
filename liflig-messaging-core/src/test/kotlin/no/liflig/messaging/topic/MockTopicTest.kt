package no.liflig.messaging.topic

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlin.concurrent.thread
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest

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
}
