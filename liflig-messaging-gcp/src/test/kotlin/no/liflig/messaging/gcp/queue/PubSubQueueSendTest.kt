package no.liflig.messaging.gcp.queue

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.string.shouldContain
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Test

internal class PubSubQueueSendTest {
  @Test
  fun `send throws when queue was constructed without a Publisher`() {
    val queue =
        PubSubQueue(subscriber = FakeSubscriberStub(), subscriptionName = "test-subscription")

    val exception = shouldThrow<IllegalStateException> { queue.send("test-message") }

    exception.message shouldContain "Provide a Publisher"
  }

  @Test
  fun `send throws when given a non-zero delay`() {
    val queue =
        PubSubQueue(subscriber = FakeSubscriberStub(), subscriptionName = "test-subscription")

    shouldThrow<UnsupportedOperationException> {
      queue.send("test-message", delay = Duration.ofSeconds(10))
    }
  }

  @Test
  fun `send accepts a zero delay`() {
    val queue =
        PubSubQueue(subscriber = FakeSubscriberStub(), subscriptionName = "test-subscription")

    // Passes the delay check, so fails on the missing Publisher instead
    shouldThrow<IllegalStateException> { queue.send("test-message", delay = Duration.ZERO) }
  }

  private class FakeSubscriberStub : SubscriberStub() {
    override fun close() {}

    override fun shutdown() {}

    override fun isShutdown(): Boolean = true

    override fun isTerminated(): Boolean = true

    override fun shutdownNow() {}

    override fun awaitTermination(duration: Long, unit: TimeUnit): Boolean = true
  }
}
