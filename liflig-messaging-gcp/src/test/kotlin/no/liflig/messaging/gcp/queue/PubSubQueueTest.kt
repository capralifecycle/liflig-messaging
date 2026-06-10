package no.liflig.messaging.gcp.queue

import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import java.time.Duration
import no.liflig.messaging.Message
import no.liflig.messaging.gcp.testutils.PubSubEmulator
import no.liflig.messaging.gcp.testutils.createPubSubEmulatorContainer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PubSubEmulatorContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PubSubQueueTest {
  lateinit var container: PubSubEmulatorContainer
  lateinit var emulator: PubSubEmulator
  lateinit var publisher: Publisher
  lateinit var subscriber: SubscriberStub
  lateinit var queue: PubSubQueue

  @BeforeAll
  fun setup() {
    container = createPubSubEmulatorContainer()
    container.start()
    emulator = PubSubEmulator(container)
    val topic = emulator.createTopic("test-topic")
    val subscription = emulator.createSubscription("test-subscription", topic)
    publisher = emulator.createPublisher(topic)
    subscriber = emulator.createSubscriberStub()
    queue = PubSubQueue(subscriber, subscription.toString(), publisher)
  }

  @AfterAll
  fun cleanup() {
    publisher.shutdown()
    subscriber.close()
    emulator.close()
    container.stop()
  }

  @Test
  fun `should be able to send and receive message`() {
    val testMessage = """{"orderId":"123","status":"CREATED"}"""

    queue.send(testMessage, customAttributes = mapOf("eventType" to "OrderCreated"))

    val message = pollSingleMessage()
    message.body shouldBe testMessage
    message.customAttributes shouldContain ("eventType" to "OrderCreated")
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
