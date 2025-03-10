package no.liflig.messaging.awssdk.queue

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.awssdk.testutils.TestMessage
import no.liflig.messaging.awssdk.testutils.TestMessagePollerObserver
import no.liflig.messaging.awssdk.testutils.TestMessageProcessor
import no.liflig.messaging.awssdk.testutils.createLocalstackContainer
import no.liflig.messaging.awssdk.testutils.createSqsClient
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.services.sqs.SqsClient

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class SqsQueueIntegrationTest {
  lateinit var localstack: LocalStackContainer
  lateinit var sqsClient: SqsClient
  lateinit var queueUrl: String
  lateinit var queue: SqsQueue

  val messageProcessor = TestMessageProcessor()
  lateinit var observer: TestMessagePollerObserver
  lateinit var messagePoller: MessagePoller

  @BeforeAll
  fun setup() {
    localstack = createLocalstackContainer()
    localstack.start()
    sqsClient = localstack.createSqsClient()
    queueUrl = sqsClient.createQueue { it.queueName("test-queue") }.queueUrl()
    queue = SqsQueue(sqsClient, queueUrl, loggingMode = MessageLoggingMode.DISABLED)

    observer = TestMessagePollerObserver(queue.observer.loggingMode ?: MessageLoggingMode.JSON)
    messagePoller = MessagePoller(queue, messageProcessor, observer = observer)
    messagePoller.start()
  }

  @BeforeEach
  fun reset() {
    sqsClient.purgeQueue { req -> req.queueUrl(queueUrl) }
    messageProcessor.reset()
    observer.reset()
  }

  @AfterAll
  fun cleanup() {
    messagePoller.close()
    localstack.stop()
  }

  @Test
  fun `MessagePoller successfully polls message from SQS`() {
    queue.send(TestMessage.SUCCESS)

    await().until { messageProcessor.successCount == 1 }
    observer.observedPollException().shouldBeNull()
    observer.observedMessageException().shouldBeNull()
  }

  @Test
  fun `MessagePoller handles failed message from SQS`() {
    queue.send(TestMessage.FAILURE)

    await().until { messageProcessor.failureCount == 1 }
    observer.observedPollException().shouldBeNull()
    observer.observedMessageException().shouldBeNull()
  }

  @Test
  fun `MessagePoller handles exception when processing message from SQS`() {
    queue.send(TestMessage.EXCEPTION)

    await().until { messageProcessor.exceptionCount == 1 }
    observer.observedPollException().shouldBeNull()
    observer.observedMessageException().shouldNotBeNull()
  }
}
