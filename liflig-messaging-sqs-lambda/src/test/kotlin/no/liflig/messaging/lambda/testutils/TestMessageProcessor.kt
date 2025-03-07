package no.liflig.messaging.lambda.testutils

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import java.util.UUID
import no.liflig.messaging.Message
import no.liflig.messaging.MessageProcessor
import no.liflig.messaging.ProcessingResult

/** Mock [MessageProcessor] implementation that counts successful and failed messages. */
internal class TestMessageProcessor : MessageProcessor {
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
internal object TestMessage {
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
