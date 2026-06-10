package no.liflig.messaging.gcp.queue

import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.gcp.testutils.PubSubEmulator
import no.liflig.messaging.gcp.testutils.TestMessage
import no.liflig.messaging.gcp.testutils.TestMessagePollerObserver
import no.liflig.messaging.gcp.testutils.TestMessageProcessor
import no.liflig.messaging.gcp.testutils.createPubSubEmulatorContainer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PubSubEmulatorContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PubSubQueueIntegrationTest {
  lateinit var container: PubSubEmulatorContainer
  lateinit var emulator: PubSubEmulator
  lateinit var publisher: Publisher
  lateinit var subscriber: SubscriberStub
  lateinit var queue: PubSubQueue

  val messageProcessor = TestMessageProcessor()
  lateinit var observer: TestMessagePollerObserver
  lateinit var messagePoller: MessagePoller

  @BeforeAll
  fun setup() {
    container = createPubSubEmulatorContainer()
    container.start()
    emulator = PubSubEmulator(container)
    val topic = emulator.createTopic("test-queue-topic")
    val subscription = emulator.createSubscription("test-queue-subscription", topic)
    publisher = emulator.createPublisher(topic)
    subscriber = emulator.createSubscriberStub()
    queue = PubSubQueue(subscriber, subscription.toString(), publisher)
    observer = TestMessagePollerObserver(queue.observer.loggingMode ?: MessageLoggingMode.JSON)
    messagePoller = MessagePoller(queue, messageProcessor, observer = observer)
    messagePoller.start()
  }

  @AfterEach
  fun reset() {
    messageProcessor.reset()
    observer.reset()
  }

  @AfterAll
  fun cleanup() {
    publisher.shutdown()
    subscriber.close()
    emulator.close()
    container.stop()
  }

  @Test
  fun `MessagePoller successfully polls message from Pub-Sub`() {
    queue.send(TestMessage.SUCCESS)

    await().until { messageProcessor.processedCount >= 1 }
    observer.observedPollException().shouldBeNull()
    observer.observedMessageException().shouldBeNull()
  }

  @Test
  fun `MessagePoller handles failed message from Pub-Sub`() {
    queue.send(TestMessage.FAILURE)

    await().until { messageProcessor.processedCount >= 1 }
    observer.observedPollException().shouldBeNull()
    observer.observedMessageException().shouldBeNull()
  }

  @Test
  fun `MessagePoller handles exception when processing message from Pub-Sub`() {
    queue.send(TestMessage.EXCEPTION)

    await().until { messageProcessor.processedCount >= 1 }
    observer.observedPollException().shouldBeNull()
    observer.observedMessageException().shouldNotBeNull()
  }

  @Test
  fun `MessagePoller handles various results from Pub-Sub`() {
    queue.send(TestMessage.SUCCESS)
    queue.send(TestMessage.FAILURE)
    queue.send(TestMessage.EXCEPTION)
    queue.send(TestMessage.SUCCESS)
    queue.send(TestMessage.FAILURE)
    queue.send(TestMessage.EXCEPTION)

    await().until { messageProcessor.processedCount >= 6 }
    messageProcessor.successCount shouldBe 2
    messageProcessor.failureCount shouldBe 2
    messageProcessor.exceptionCount shouldBe 2
  }
}
