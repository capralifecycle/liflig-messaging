package no.liflig.messaging

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.concurrent.withLock
import no.liflig.logging.LoggingContext
import no.liflig.logging.getCopyOfLoggingContext
import no.liflig.logging.getLogger
import no.liflig.logging.withLoggingContext
import no.liflig.messaging.queue.MockQueue
import no.liflig.messaging.testutils.TestMessage
import no.liflig.messaging.testutils.TestMessagePollerObserver
import no.liflig.messaging.testutils.TestMessageProcessor
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

private val log = getLogger()

/** Test if MessagePoller correctly invokes the observer */
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
internal class MessagePollerObserverTest {
  val queue = MockQueue()
  val testProcessor = TestMessageProcessor()
  val observer = TestMessagePollerObserver()
  val messagePoller = MessagePoller(queue, testProcessor, observer = observer, sleep = {})

  @BeforeEach
  fun setup() {
    messagePoller.start()
  }

  @AfterEach
  fun tearDown() {
    messagePoller.close()
  }

  @Test
  fun `successful message invokes onMessageSuccess`() {
    repeat(3) { queue.send(TestMessage.SUCCESS) }

    await().until { queue.sentMessages.isEmpty() }

    observer.successCount shouldBe 3
  }

  @Test
  fun `message failure invokes onMessageFailure`() {
    repeat(3) { queue.send(TestMessage.FAILURE) }

    await().until { queue.sentMessages.isEmpty() }

    observer.failureCount shouldBe 3
  }

  @Test
  fun `exception invokes onMessageException`() {
    repeat(3) { queue.send(TestMessage.EXCEPTION) }

    await().until { queue.sentMessages.isEmpty() }

    observer.exceptionCount shouldBe 3
  }

  @Test
  fun `processing a message invokes wrapMessageProcessing`() {
    queue.send(TestMessage.SUCCESS)
    queue.send(TestMessage.FAILURE)
    queue.send(TestMessage.EXCEPTION)

    await().until { queue.sentMessages.isEmpty() }

    observer.wrappedProcessingCount shouldBe 3
  }

  @Test
  fun `processing a message invokes onMessageProcessing`() {
    queue.send(TestMessage.SUCCESS)
    queue.send(TestMessage.FAILURE)
    queue.send(TestMessage.EXCEPTION)

    await().until { queue.sentMessages.isEmpty() }

    observer.processingCount shouldBe 3
  }

  @Test
  fun `invokes onStartup`() {
    val obs = TestMessagePollerObserver()
    MessagePoller(queue, testProcessor, observer = obs).use {
      it.start()
      obs.startupCount shouldBe 1
    }
  }

  @Test
  fun `invokes onShutdown`() {
    val messagePoller = MessagePoller(queue, testProcessor, observer = observer)
    messagePoller.start()
    messagePoller.close()
    observer.shutdownCount shouldBe 1
  }

  @Test
  fun `DefaultMessagePollerObserver adds expected fields to logging context`() {
    val pollerName = "CustomPollerNameForTest"
    val queueMessageId = MessageId("f04be04e-2dd0-488f-8ff0-be49a3ddb215")

    var loggingContext: LoggingContext? = null

    val processor = MessageProcessor {
      loggingContext = getCopyOfLoggingContext()
      ProcessingResult.Success
    }

    val queue = MockQueue()

    MessagePoller(queue, processor, name = pollerName).use { poller ->
      poller.start()

      queue.lock.withLock {
        queue.sentMessages.add(
            Message(
                id = queueMessageId,
                body = """{"test":true}""",
                systemAttributes = emptyMap(),
                customAttributes = emptyMap(),
            ),
        )
      }

      await().until { loggingContext != null }

      loggingContext.shouldNotBeNull()

      val logOutput = captureStdout { withLoggingContext(loggingContext) { log.info { "Test" } } }

      logOutput.shouldContain(
          """
            "queueMessageId":"${queueMessageId}"
          """
              .trimIndent(),
      )
      logOutput.shouldContain(
          """
            "messagePollerName":"${pollerName}"
          """
              .trimIndent(),
      )
    }
  }

  @Test
  fun `QuietMessagePollerObserver logs nothing for successful message processing`() {
    val queue = MockQueue()

    MessagePoller(
            queue,
            testProcessor,
            observer = QuietMessagePollerObserver(pollerName = "QuietMessagePoller"),
        )
        .use { poller ->
          poller.start()

          val logOutput = captureStdout {
            queue.send(TestMessage.SUCCESS)
            await().until { queue.sentMessages.isEmpty() }
          }

          logOutput shouldBe ""
        }
  }
}

private inline fun captureStdout(block: () -> Unit): String {
  val originalStdout = System.out

  // We redirect System.out to our own output stream, so we can capture the log output
  val outputStream = ByteArrayOutputStream()
  System.setOut(PrintStream(outputStream))

  try {
    block()
  } finally {
    System.setOut(originalStdout)
  }

  return outputStream.toString("UTF-8")
}
