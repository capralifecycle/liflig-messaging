package no.liflig.messaging.lambda

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import java.util.UUID
import no.liflig.messaging.api.Message
import no.liflig.messaging.api.MessageProcessor
import no.liflig.messaging.api.ProcessingResult
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class LambdaSqsEventHandlerTest {
  private val processor = TestMessageProcessor()

  @BeforeEach
  fun reset() {
    processor.reset()
  }

  @Test
  fun `successful messages are not marked as failed`() {
    val sqsEvent =
        createSqsEvent(
            TestMessage.success(),
            TestMessage.success(),
            TestMessage.success(),
        )
    val result = handleLambdaSqsEvent(sqsEvent, processor)
    result.batchItemFailures.shouldBeEmpty()

    processor.successCount shouldBe 3
    processor.failureCount shouldBe 0
    processor.exceptionCount shouldBe 0
  }

  @Test
  fun `failed messages are included in batch response`() {
    val failedMessage = TestMessage.failure()
    val sqsEvent =
        createSqsEvent(
            TestMessage.success(),
            failedMessage,
            TestMessage.success(),
        )
    val result = handleLambdaSqsEvent(sqsEvent, processor)
    result.shouldHaveFailedMessages(failedMessage)

    processor.successCount shouldBe 2
    processor.failureCount shouldBe 1
    processor.exceptionCount shouldBe 0
  }

  @Test
  fun `messages that throw exceptions are included in batch response`() {
    val exceptionMessage = TestMessage.exception()
    // To test that both exception and normal failure are included
    val failedMessage = TestMessage.failure()
    val sqsEvent =
        createSqsEvent(
            TestMessage.success(),
            exceptionMessage,
            failedMessage,
            TestMessage.success(),
        )
    val result = handleLambdaSqsEvent(sqsEvent, processor)
    result.shouldHaveFailedMessages(exceptionMessage, failedMessage)

    processor.successCount shouldBe 2
    processor.failureCount shouldBe 1
    processor.exceptionCount shouldBe 1
  }

  /** Mock [MessageProcessor] implementation that counts successful and failed messages. */
  private class TestMessageProcessor : MessageProcessor {
    var successCount = 0
    var failureCount = 0
    var exceptionCount = 0

    override fun process(message: Message): ProcessingResult {
      return when (message.body) {
        TestMessage.SUCCESS -> {
          successCount++
          ProcessingResult.Success
        }
        TestMessage.FAILURE -> {
          failureCount++
          ProcessingResult.Failure(retry = true)
        }
        TestMessage.EXCEPTION -> {
          exceptionCount++
          throw Exception("Processing failed due to exception")
        }
        else -> ProcessingResult.Success
      }
    }

    fun reset() {
      successCount = 0
      failureCount = 0
      exceptionCount = 0
    }
  }

  /**
   * In order to trigger successes/failures/exception in [TestMessageProcessor], we use these
   * pre-defined message bodies to decide how to process them.
   */
  private object TestMessage {
    const val SUCCESS = """{"type":"SUCCESS"}"""
    const val FAILURE = """{"type":"FAILURE"}"""
    const val EXCEPTION = """{"type":"EXCEPTION"}"""

    fun success() = createMessage(SUCCESS)

    fun failure() = createMessage(FAILURE)

    fun exception() = createMessage(EXCEPTION)

    private fun createMessage(body: String): SQSEvent.SQSMessage {
      return SQSEvent.SQSMessage().apply {
        this.body = body
        messageId = UUID.randomUUID().toString()
        receiptHandle = UUID.randomUUID().toString()
      }
    }
  }

  private fun createSqsEvent(vararg messages: SQSEvent.SQSMessage): SQSEvent {
    return SQSEvent().apply { records = messages.toList() }
  }

  private fun SQSBatchResponse.shouldHaveFailedMessages(vararg messages: SQSEvent.SQSMessage) {
    batchItemFailures.shouldContainExactly(messages.map { BatchItemFailure(it.messageId) })
  }
}
