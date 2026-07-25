package no.liflig.messaging.utils

import io.mockk.spyk
import io.mockk.verifyOrder
import no.liflig.messaging.Message
import no.liflig.messaging.MessageId
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.MessagePollerObserver
import no.liflig.messaging.ProcessingResult
import no.liflig.messaging.chained
import no.liflig.messaging.queue.MockQueue
import org.junit.jupiter.api.Test

internal class NoopObserver : MessagePollerObserver {
  @Suppress("RedundantOverride")
  override fun onPoll(messages: List<Message>) {
    super.onPoll(messages)
  }

  override fun <ReturnT> wrapMessageProcessing(
      message: Message,
      messageProcessingBlock: () -> ReturnT,
  ): ReturnT = messageProcessingBlock()

  override fun <ReturnT> wrapPoller(pollerBlock: () -> ReturnT): ReturnT = pollerBlock()
}

internal class ChainedMessageObserverTest {
  val queue = spyk(MockQueue())
  val observer1 = spyk(NoopObserver())
  val observer2 = spyk(NoopObserver())

  val observer = chained(observer1, observer2)
  val poller =
      MessagePoller(
          queue,
          { m ->
            if (m.body == "success") {
              ProcessingResult.Success
            } else {
              ProcessingResult.Failure(false)
            }
          },
          observer = observer,
      )
  val message =
      Message(
          id = MessageId("123"),
          body = "success",
          systemAttributes = emptyMap(),
          customAttributes = emptyMap(),
      )

  @Test
  fun `test onPoll`() {
    queue.sentMessages.addLast(message)

    poller.poll()
    verifyOrder {
      queue.poll()
      observer1.onPoll(eq(listOf(message)))
      observer2.onPoll(eq(listOf(message)))
    }
  }

  @Test
  fun `test wrapMessageProcessing order`() {
    queue.sentMessages.addLast(message)

    poller.poll()
    verifyOrder {
      queue.poll()
      observer1.wrapMessageProcessing<Boolean>(message, any())
      observer2.wrapMessageProcessing<Boolean>(message, any())
    }
  }

  @Test
  fun `test wrapPoller order`() {
    queue.sentMessages.addLast(message)

    poller.start()
    queue.awaitProcessed(1)
    verifyOrder {
      observer1.wrapPoller<Boolean>(any())
      observer2.wrapPoller<Boolean>(any())
      queue.poll()
    }
    poller.close()
  }
}
