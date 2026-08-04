package no.liflig.messaging

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import no.liflig.messaging.queue.MockQueue
import no.liflig.messaging.testutils.TestMessage
import no.liflig.messaging.testutils.TestMessagePollerObserver
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

  @Test
  fun `stopPredicate can stop poller thread`() {
    val observer = TestMessagePollerObserver()

    MessagePoller(queue, testProcessor, observer = observer, stopPredicate = { true }).use {
        messagePoller ->
      messagePoller.start()

      queue.send(TestMessage.EXCEPTION)

      await().until { observer.threadStoppedCount > 0 }
    }
  }

  @Test
  fun `test backoff`() {
    val queue = MockQueue()
    val slept = AtomicLong(0)

    MessagePoller(
            queue = queue,
            messageProcessor = { _ ->
              ProcessingResult.Failure(retry = false, backoff = Duration.ofSeconds(17))
            },
            sleep = slept::addAndGet,
        )
        .use { messagePoller ->
          messagePoller.start()

          queue.send("1")
          queue.awaitFailedWithoutRetry(1)
          queue.send("2")
          queue.awaitFailedWithoutRetry(1)
          queue.send("3")
          queue.awaitFailedWithoutRetry(1)
        }

    slept.get() shouldBe (17000 * 3)
  }

  @Test
  fun `test batched backoff`() {
    val queue = MockQueue()
    val slept = AtomicLong(0)

    MessagePoller(
            queue = queue,
            messageProcessor = { _ ->
              ProcessingResult.Failure(retry = false, backoff = Duration.ofSeconds(17))
            },
            sleep = slept::addAndGet,
        )
        .use { messagePoller ->
          queue.send("1")
          queue.send("2")
          queue.send("3")

          messagePoller.start()

          queue.awaitFailedWithoutRetry(3)
        }

    slept.get() shouldBe (17000)
  }

  @Test
  fun `test no backoff`() {
    val queue = MockQueue()
    val slept = AtomicLong(0)

    MessagePoller(
            queue = queue,
            messageProcessor = { _ -> ProcessingResult.Failure(retry = false) },
            sleep = slept::addAndGet,
        )
        .use { messagePoller ->
          messagePoller.start()

          queue.send("1")
          queue.send("2")
          queue.send("3")

          queue.awaitFailedWithoutRetry(3)
        }

    slept.get() shouldBe (0)
  }
}
