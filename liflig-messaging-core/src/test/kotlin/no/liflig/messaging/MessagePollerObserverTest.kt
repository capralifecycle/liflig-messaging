package no.liflig.messaging

import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import kotlin.concurrent.withLock
import no.liflig.logging.LogField
import no.liflig.logging.field
import no.liflig.logging.getLoggingContext
import no.liflig.messaging.queue.MockQueue
import no.liflig.messaging.testutils.TestMessage
import no.liflig.messaging.testutils.TestMessagePollerObserver
import no.liflig.messaging.testutils.TestMessageProcessor
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** Test if MessagePoller correctly invokes the observer */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class MessagePollerObserverTest {
  lateinit var queue: MockQueue
  lateinit var testProcessor: TestMessageProcessor
  lateinit var observer: TestMessagePollerObserver
  lateinit var messagePoller: MessagePoller

  @BeforeAll
  fun setup() {
    queue = MockQueue()
    testProcessor = TestMessageProcessor()
    observer = TestMessagePollerObserver()
    messagePoller = MessagePoller(queue, testProcessor, observer = observer)
    messagePoller.start()
  }

  @BeforeEach
  fun reset() {
    queue.sentMessages.clear()
    testProcessor.reset()
    observer.reset()
  }

  @AfterAll
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
    MessagePoller(queue, testProcessor, observer = observer).use {
      messagePoller.start()

      observer.startupCount shouldBe 1
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
    val queueMessageId = "f04be04e-2dd0-488f-8ff0-be49a3ddb215"

    var loggingContextFields: List<LogField>? = null

    val processor =
        object : MessageProcessor {
          override fun process(message: Message): ProcessingResult {
            loggingContextFields = getLoggingContext()
            return ProcessingResult.Success
          }
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

      await().until { loggingContextFields != null }

      loggingContextFields.shouldContainExactlyInAnyOrder(
          field("queueMessageId", queueMessageId),
          field("messagePollerName", pollerName),
      )
    }
  }
}
