package no.liflig.messaging.awssdk.queue

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import no.liflig.messaging.awssdk.testutils.createLocalstackContainer
import no.liflig.messaging.awssdk.testutils.createSqsClient
import no.liflig.messaging.awssdk.testutils.readResourceFile
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.services.sqs.SqsClient

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class SqsQueueTest {
  lateinit var queuUrl: String
  lateinit var client: SqsClient
  lateinit var localstack: LocalStackContainer

  @BeforeAll
  fun setup() {
    localstack = createLocalstackContainer()
    localstack.start()
    client = localstack.createSqsClient()
    queuUrl = client.createQueue { it.queueName("test-queue") }.queueUrl()
  }

  @AfterAll
  fun cleanup() {
    localstack.stop()
  }

  @Test
  fun `should be able to send and receive message`() {
    val testMessage = readResourceFile("TestMessage.json")
    val queue = SqsQueue(client, queuUrl)

    queue.send(testMessage)

    val message = queue.poll().shouldHaveSize(1).first()
    message.body shouldBe testMessage
    shouldNotThrow<IllegalStateException> { message.getSqsSentTimestamp() }
  }
}
