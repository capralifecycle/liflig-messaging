package no.liflig.messaging.awssdk.queue

import no.liflig.messaging.MessagePoller
import no.liflig.messaging.awssdk.testutils.MockProcessor
import no.liflig.messaging.awssdk.testutils.createLocalstackContainer
import no.liflig.messaging.awssdk.testutils.createSqsClient
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.services.sqs.SqsClient

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class SqsQueueIntegrationTest {
  lateinit var queueUrl: String
  lateinit var client: SqsClient
  lateinit var localstack: LocalStackContainer

  @BeforeAll
  fun setup() {
    localstack = createLocalstackContainer()
    localstack.start()
    client = localstack.createSqsClient()
    queueUrl = client.createQueue { it.queueName("test-queue") }.queueUrl()
  }

  @AfterAll
  fun cleanup() {
    localstack.stop()
  }

  @Test
  fun `works with MessagePoller`() {
    val queue = SqsQueue(client, queueUrl)
    val mockProcessor = MockProcessor()

    MessagePoller(queue, mockProcessor).start()
    repeat(3) { queue.send("Message-$it") }

    await().until { mockProcessor.hasProcessed(3) }
  }
}
