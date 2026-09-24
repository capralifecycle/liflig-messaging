package no.liflig.messaging.gcp.queue

import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.SubscriptionName
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import java.time.Duration
import java.time.Instant
import no.liflig.messaging.Message
import no.liflig.messaging.backoff.BackoffConfig
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
  lateinit var subscription: SubscriptionName
  lateinit var queue: PubSubQueue

  @BeforeAll
  fun setup() {
    container = createPubSubEmulatorContainer()
    container.start()
    emulator = PubSubEmulator(container)
    val topic = emulator.createTopic("test-topic")
    subscription = emulator.createSubscription("test-subscription", topic)
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

  @Test
  fun `retry redelivers message after backoff`() {
    // The subscription's ack deadline is 10 seconds. We use a shorter backoff, so we can tell that
    // redelivery was caused by the backoff, and not by the original ack deadline expiring.
    val backoffQueue =
        PubSubQueue(
            subscriber,
            subscription.toString(),
            publisher,
            backoffConfig = BackoffConfig(initialIntervalSeconds = 3),
        )
    backoffQueue.send("""{"orderId":"789","status":"CREATED"}""")
    val message = pollSingleMessage(queue = backoffQueue, ack = false)

    val retriedAt = Instant.now()
    backoffQueue.retry(message)

    val redelivered = pollSingleMessage(queue = backoffQueue)
    val redeliveredAfter = Duration.between(retriedAt, Instant.now())
    redelivered.id shouldBe message.id
    redeliveredAfter shouldBeGreaterThan Duration.ofSeconds(2)
    redeliveredAfter shouldBeLessThan Duration.ofSeconds(8)
  }

  /**
   * Polls the queue, retrying until a single message is received (or the timeout is hit). Polled
   * messages are acknowledged by default, so they are not redelivered to later tests once the
   * subscription's ack deadline expires.
   */
  private fun pollSingleMessage(queue: PubSubQueue = this.queue, ack: Boolean = true): Message {
    var messages: List<Message> = emptyList()
    await().atMost(Duration.ofSeconds(10)).until {
      messages = queue.poll()
      if (ack) {
        messages.forEach { queue.delete(it) }
      }
      messages.size == 1
    }
    return messages.single()
  }
}
