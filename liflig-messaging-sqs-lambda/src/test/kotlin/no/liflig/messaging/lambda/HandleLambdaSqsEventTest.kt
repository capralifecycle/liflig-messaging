package no.liflig.messaging.lambda

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.shouldBe
import no.liflig.messaging.lambda.testutils.TestMessage
import no.liflig.messaging.lambda.testutils.TestMessageProcessor
import no.liflig.messaging.lambda.testutils.createSqsEvent
import no.liflig.messaging.lambda.testutils.shouldHaveFailedMessages
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class HandleLambdaSqsEventTest {
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
}
