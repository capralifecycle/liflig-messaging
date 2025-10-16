package no.liflig.messaging.testutils

import no.liflig.messaging.DefaultMessagePollerObserver
import no.liflig.messaging.Message
import no.liflig.messaging.ProcessingResult

internal class TestMessagePollerObserver : DefaultMessagePollerObserver() {

  var exceptionCount = 0
  var failureCount = 0
  var pollCount = 0
  var processingCount = 0
  var shutdownCount = 0
  var startupCount = 0
  var successCount = 0
  var threadStoppedCount = 0
  var wrappedProcessingCount = 0

  fun reset() {
    exceptionCount = 0
    failureCount = 0
    pollCount = 0
    processingCount = 0
    shutdownCount = 0
    startupCount = 0
    successCount = 0
    threadStoppedCount = 0
    wrappedProcessingCount = 0
  }

  override fun onPollerStartup() {
    startupCount++
  }

  override fun onPoll(messages: List<Message>) {
    pollCount++
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
    processingCount++
  }

  override fun onMessageException(message: Message, exception: Throwable) {
    exceptionCount++
  }

  override fun onPollerThreadStopped(cause: Throwable?) {
    threadStoppedCount++
  }

  override fun <ReturnT> wrapMessageProcessing(
      message: Message,
      messageProcessingBlock: () -> ReturnT,
  ): ReturnT {
    wrappedProcessingCount++
    return messageProcessingBlock()
  }
}
