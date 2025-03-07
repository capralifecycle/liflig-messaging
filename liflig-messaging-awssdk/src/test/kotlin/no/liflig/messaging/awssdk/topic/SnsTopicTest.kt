package no.liflig.messaging.awssdk.topic

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import no.liflig.messaging.awssdk.queue.SqsQueue
import no.liflig.messaging.awssdk.testutils.createLocalstackContainer
import no.liflig.messaging.awssdk.testutils.createSnsClient
import no.liflig.messaging.awssdk.testutils.createSqsClient
import no.liflig.messaging.awssdk.testutils.readResourceFile
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class SnsTopicTest {
  lateinit var topicArn: String
  lateinit var queueUrl: String
  lateinit var snsClient: SnsClient
  lateinit var sqsClient: SqsClient
  lateinit var localstack: LocalStackContainer

  /** Create a SNS topic, and add a subscription to a SQS queue for verifying delivery */
  @BeforeAll
  fun setup() {
    localstack = createLocalstackContainer()
    localstack.start()
    snsClient = localstack.createSnsClient()
    sqsClient = localstack.createSqsClient()
    topicArn = snsClient.createTopic { it.name("test-topic") }.topicArn()
    queueUrl = sqsClient.createQueue { it.queueName("test-queue") }.queueUrl()

    snsClient.subscribe {
      it.topicArn(topicArn)
          .endpoint(sqsClient.getQueueArn(queueUrl))
          .protocol("sqs")
          .attributes(mapOf("RawMessageDelivery" to "true"))
    }
  }

  @AfterAll
  fun cleanup() {
    localstack.stop()
  }

  @Test
  fun `publish successfully delivers messages to sns topic`() {
    val testMessage = readResourceFile("TestMessage.json")
    val topic = SnsTopic(snsClient, topicArn)
    val queue = SqsQueue(sqsClient, queueUrl)

    topic.publish(message = testMessage)

    val messages = queue.poll() shouldHaveSize 1
    messages.first().body shouldBe testMessage
  }
}

internal fun SqsClient.getQueueArn(queueUrl: String) =
    getQueueAttributes { it.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN) }
        .attributes()[QueueAttributeName.QUEUE_ARN]
