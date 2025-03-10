package no.liflig.messaging.testutils

import no.liflig.messaging.DefaultMessagePollerObserver
import no.liflig.messaging.Message
import no.liflig.messaging.ProcessingResult

internal class TestMessagePollerObserver : DefaultMessagePollerObserver() {

  var exceptionCount = 0
  var failureCount = 0
  var messageProcessing = 0
  var shutdownCount = 0
  var startupCount = 0
  var successCount = 0
  var wrappedProcessingCount = 0

  fun reset() {
    exceptionCount = 0
    failureCount = 0
    messageProcessing = 0
    shutdownCount = 0
    startupCount = 0
    successCount = 0
    wrappedProcessingCount = 0
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

  override fun onMessageProcessing(message: Message) {
    messageProcessing++
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
