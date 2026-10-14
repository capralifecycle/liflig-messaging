package no.liflig.messaging

import io.kotest.assertions.nondeterministic.continually
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import java.time.Duration
import java.util.concurrent.CountDownLatch
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
  suspend fun `test backoff`() {
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

    eventually { slept.get() shouldBe (17000 * 3) }
  }

  @Test
  suspend fun `test batched backoff`() {
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

    eventually { slept.get() shouldBe (17000) }
  }

  @Test
  suspend fun `test no backoff`() {
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

    continually(200) { slept.get() shouldBe 0 }
  }

  @Test
  fun `graceful shutdown should stop at timeout`() {
    val queue = MockQueue()

    /*
     We want the following sequence to occur:
     - Messages are sent to queue
     - MessagePoller polls the queue
     - Shutdown hook is called
     - MessageProcessor processes messages until shutdown hook times out and terminates
    */

    val messagesPolled = CountDownLatch(1)
    val shutdownHookCalled = CountDownLatch(1)

    MessagePoller(
            queue = queue,
            messageProcessor = { _ ->
              messagesPolled.countDown()
              shutdownHookCalled.await()
              Thread.sleep(100)
              ProcessingResult.Success
            },
            shutdownTimeout = Duration.ofMillis(250),
        )
        .use { messagePoller ->
          listOf("1", "2", "3", "4", "5").forEach { queue.send(it) }
          messagePoller.start()

          messagesPolled.await()
          // Invoke the shutdown hook manually. We don't test the actually test everything
          // end-to-end
          messagePoller.shutdownHook?.start()
          shutdownHookCalled.countDown()

          queue.awaitProcessed(2, Duration.ofSeconds(2))
          queue.expectSent(3)
        }
  }
}
