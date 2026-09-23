package no.liflig.messaging.gcp.queue

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutures
import com.google.api.core.SettableApiFuture
import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.grpc.Status
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.every
import io.mockk.mockk
import java.time.Duration
import no.liflig.messaging.MessageId
import no.liflig.messaging.queue.MessageSendingException
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

  @Test
  fun `send returns the published message ID`() {
    val queue = createQueueWithPublisher(ApiFutures.immediateFuture("published-id"))

    queue.send("test-message") shouldBe MessageId("published-id")
  }

  @Test
  fun `send exception has the publish error as cause, not ExecutionException`() {
    val publishError = NotFoundException(null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false)
    val queue = createQueueWithPublisher(ApiFutures.immediateFailedFuture(publishError))

    val exception = shouldThrow<MessageSendingException> { queue.send("test-message") }

    exception.cause shouldBeSameInstanceAs publishError
  }

  @Test
  fun `send restores interrupt flag when interrupted`() {
    // Never completes, like a publish waiting for a response
    val queue = createQueueWithPublisher(SettableApiFuture.create())

    Thread.currentThread().interrupt()
    try {
      val exception = shouldThrow<MessageSendingException> { queue.send("test-message") }

      exception.cause.shouldBeInstanceOf<InterruptedException>()
      Thread.currentThread().isInterrupted shouldBe true
    } finally {
      // Clear the interrupt flag, so it doesn't leak into other tests
      Thread.interrupted()
    }
  }

  private fun createQueueWithPublisher(publishResult: ApiFuture<String>): PubSubQueue {
    val publisher = mockk<Publisher>()
    every { publisher.topicNameString } returns "projects/test-project/topics/test-topic"
    every { publisher.publish(any()) } returns publishResult

    return PubSubQueue(
        subscriber = mockk<SubscriberStub>(),
        subscriptionName = "test-subscription",
        publisher = publisher,
    )
  }
}
