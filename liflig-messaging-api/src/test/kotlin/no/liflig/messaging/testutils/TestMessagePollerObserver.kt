package no.liflig.messaging.testutils

import no.liflig.messaging.DefaultMessagePollerObserver
import no.liflig.messaging.Message
import no.liflig.messaging.ProcessingResult

internal class TestMessagePollerObserver : DefaultMessagePollerObserver() {

  var failureCount = 0
  var successCount = 0
  var exceptionCount = 0
  var wrappedProcessingCount = 0
  var startupCount = 0
  var shutdownCount = 0

  fun reset() {
    failureCount = 0
    successCount = 0
    exceptionCount = 0
    wrappedProcessingCount = 0
    startupCount = 0
    shutdownCount = 0
  }

  override fun onPollerStartup() {
    startupCount++
  }

  override fun onPollerShutdown() {
    shutdownCount++
  }

  override fun onMessageFailure(message: Message, result: ProcessingResult.Failure) {
    failureCount++
  }

  override fun onMessageSuccess(message: Message) {
    successCount++
  }

  override fun onMessageException(message: Message, exception: Throwable) {
    exceptionCount++
  }

  override fun <ReturnT> wrapMessageProcessing(
      message: Message,
      messageProcessingBlock: () -> ReturnT
  ): ReturnT {
    wrappedProcessingCount++
    return messageProcessingBlock()
  }
}
