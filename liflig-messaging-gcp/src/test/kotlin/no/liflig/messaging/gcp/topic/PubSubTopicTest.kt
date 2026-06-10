package no.liflig.messaging.gcp.topic

import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.kotest.matchers.shouldBe
import java.time.Duration
import no.liflig.messaging.Message
import no.liflig.messaging.gcp.queue.PubSubQueue
import no.liflig.messaging.gcp.testutils.PubSubEmulator
import no.liflig.messaging.gcp.testutils.createPubSubEmulatorContainer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PubSubEmulatorContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PubSubTopicTest {
  lateinit var container: PubSubEmulatorContainer
  lateinit var emulator: PubSubEmulator
  lateinit var publisher: Publisher
  lateinit var subscriber: SubscriberStub
  lateinit var topic: PubSubTopic
  lateinit var queue: PubSubQueue

  /** Create a topic, and a subscription on it for verifying delivery. */
  @BeforeAll
  fun setup() {
    container = createPubSubEmulatorContainer()
    container.start()
    emulator = PubSubEmulator(container)
    val topicName = emulator.createTopic("test-topic")
    val subscription = emulator.createSubscription("test-subscription", topicName)
    publisher = emulator.createPublisher(topicName)
    subscriber = emulator.createSubscriberStub()
    topic = PubSubTopic(publisher)
    queue = PubSubQueue(subscriber, subscription.toString())
  }

  @AfterAll
  fun cleanup() {
    publisher.shutdown()
    subscriber.close()
    emulator.close()
    container.stop()
  }

  @Test
  fun `publish successfully delivers message to subscription`() {
    val testMessage = """{"orderId":"123","status":"CREATED"}"""

    topic.publish(message = testMessage)

    val message = pollSingleMessage()
    message.body shouldBe testMessage
  }

  /** Polls the queue, retrying until a single message is received (or the timeout is hit). */
  private fun pollSingleMessage(): Message {
    var messages: List<Message> = emptyList()
    await().atMost(Duration.ofSeconds(10)).until {
      messages = queue.poll()
      messages.size == 1
    }
    return messages.single()
  }
}
