package no.liflig.messaging.testutils

import no.liflig.messaging.Message
import no.liflig.messaging.MessageProcessor
import no.liflig.messaging.ProcessingResult

internal class MockProcessor : MessageProcessor {
  val processedMessages = mutableListOf<Message>()

  override fun process(message: Message): ProcessingResult {
    processedMessages.add(message)
    return ProcessingResult.Success
  }

  fun hasProcessed(messageCount: Int): Boolean = processedMessages.size == messageCount
}
