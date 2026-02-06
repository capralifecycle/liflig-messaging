package no.liflig.messaging.queue

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import java.util.UUID
import kotlin.concurrent.thread
import no.liflig.messaging.Message
import no.liflig.messaging.MessageId
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.ProcessingResult
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

internal class MockQueueTest {
  val queue = MockQueue()

  @BeforeEach
  fun reset() {
    queue.clear()
  }

  @Test
  fun `messages that fail processing with retry disabled are added to failedMessages`() {
    val message = createMessage("test")
    queue.sentMessages.add(message)

    val messagePoller =
        MessagePoller(
            queue,
            messageProcessor = { ProcessingResult.Failure(retry = false) },
        )
    messagePoller.poll()

    queue.failedMessages.shouldContainExactly(message)
    queue.sentMessages.shouldBeEmpty()
    queue.processedMessages.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitProcessed`() {
    val messages = listOf(createMessage("1"), createMessage("2"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitProcessed(messages.size) }

    val producer = thread {
      messages.forEach {
        queue.delete(it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
    queue.processedMessages.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitFailed`() {
    val messages = listOf(createMessage("1"), createMessage("2"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitFailed(messages.size) }

    val producer = thread {
      messages.forEach {
        queue.retry(it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
    queue.failedMessages.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitSent`() {
    val messageBodies = listOf("1", "2")

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitSent(messageBodies.size) }

    val producer = thread {
      messageBodies.forEach {
        queue.send(messageBody = it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().map { it.body }.shouldBe(messageBodies)
    queue.sentMessages.shouldBeEmpty()
  }
}

private fun createMessage(body: String) =
    Message(
        id = MessageId(UUID.randomUUID().toString()),
        body = body,
        systemAttributes = emptyMap(),
        customAttributes = emptyMap(),
    )
