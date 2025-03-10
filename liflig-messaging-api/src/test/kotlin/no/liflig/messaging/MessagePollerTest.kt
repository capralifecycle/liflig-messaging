package no.liflig.messaging

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import no.liflig.messaging.queue.MockQueue
import no.liflig.messaging.testutils.TestMessage
import no.liflig.messaging.testutils.TestMessageProcessor
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class MessagePollerTest {
  lateinit var queue: MockQueue
  lateinit var testProcessor: TestMessageProcessor

  @BeforeEach
  fun setup() {
    queue = MockQueue()
    testProcessor = TestMessageProcessor()
  }

  @Test
  fun `successfully takes messages from queue and sends to messageProcessor`() {
    MessagePoller(queue, testProcessor).use {
      it.start()
      repeat(3) { queue.send(TestMessage.SUCCESS) }
      await().until { queue.sentMessages.isEmpty() }

      testProcessor.successCount shouldBe 3
      queue.processedMessages shouldHaveSize 3
      queue.sentMessages shouldHaveSize 0
    }
  }
}
