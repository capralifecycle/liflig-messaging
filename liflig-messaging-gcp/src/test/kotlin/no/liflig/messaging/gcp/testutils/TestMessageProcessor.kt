package no.liflig.messaging.gcp.testutils

import no.liflig.messaging.Message
import no.liflig.messaging.MessageProcessor
import no.liflig.messaging.ProcessingResult

/** Mock [MessageProcessor] implementation that counts successful and failed messages. */
internal class TestMessageProcessor : MessageProcessor {
  var processedCount = 0
  var successCount = 0
  var failureCount = 0
  var exceptionCount = 0

  override fun process(message: Message): ProcessingResult {
    processedCount++
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
    processedCount = 0
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
}
