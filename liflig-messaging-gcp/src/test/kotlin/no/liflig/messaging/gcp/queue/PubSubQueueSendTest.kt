package no.liflig.messaging.gcp.queue

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.string.shouldContain
import io.mockk.mockk
import java.time.Duration
import org.junit.jupiter.api.Test

internal class PubSubQueueSendTest {
  @Test
  fun `send throws when queue was constructed without a Publisher`() {
    val queue =
        PubSubQueue(subscriber = mockk<SubscriberStub>(), subscriptionName = "test-subscription")

    val exception = shouldThrow<IllegalStateException> { queue.send("test-message") }

    exception.message shouldContain "Provide a Publisher"
  }

  @Test
  fun `send throws when given a non-zero delay`() {
    val queue =
        PubSubQueue(subscriber = mockk<SubscriberStub>(), subscriptionName = "test-subscription")

    shouldThrow<UnsupportedOperationException> {
      queue.send("test-message", delay = Duration.ofSeconds(10))
    }
  }

  @Test
  fun `send throws when given system attributes`() {
    val queue =
        PubSubQueue(subscriber = mockk<SubscriberStub>(), subscriptionName = "test-subscription")

    shouldThrow<UnsupportedOperationException> {
      queue.send("test-message", systemAttributes = mapOf("AWSTraceHeader" to "trace"))
    }
  }

  @Test
  fun `send accepts a zero delay`() {
    val queue =
        PubSubQueue(subscriber = mockk<SubscriberStub>(), subscriptionName = "test-subscription")

    // Passes the delay check, so fails on the missing Publisher instead
    shouldThrow<IllegalStateException> { queue.send("test-message", delay = Duration.ZERO) }
  }
}
