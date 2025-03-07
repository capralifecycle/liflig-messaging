package no.liflig.messaging

import io.kotest.matchers.collections.shouldHaveSize
import no.liflig.messaging.queue.MockQueue
import no.liflig.messaging.testutils.MockProcessor
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test

internal class MessagePollerTest {
  @Test
  fun `successfully takes messages from queue and sends to messageProcessor`() {
    val queue = MockQueue()
    val mockProcessor = MockProcessor()

    MessagePoller(queue, mockProcessor).start()
    repeat(3) { queue.send("Message-$it") }

    await().until { mockProcessor.hasProcessed(3) }
    queue.processedMessages shouldHaveSize 3
    queue.sentMessages shouldHaveSize 0
  }
}
