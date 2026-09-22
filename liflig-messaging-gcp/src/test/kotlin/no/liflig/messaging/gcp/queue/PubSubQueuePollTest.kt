package no.liflig.messaging.gcp.queue

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutures
import com.google.api.core.SettableApiFuture
import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.ApiCallContext
import com.google.api.gax.rpc.DeadlineExceededException
import com.google.api.gax.rpc.NotFoundException
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PullResponse
import com.google.pubsub.v1.ReceivedMessage
import io.grpc.Status
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import java.time.Duration
import org.junit.jupiter.api.Test

internal class PubSubQueuePollTest {
  @Test
  fun `poll returns received messages`() {
    val response =
        PullResponse.newBuilder()
            .addReceivedMessages(
                ReceivedMessage.newBuilder()
                    .setAckId("ack-id")
                    .setMessage(PubsubMessage.newBuilder().setMessageId("message-id")),
            )
            .build()
    val queue = createQueue(ApiFutures.immediateFuture(response))

    val messages = queue.poll()

    messages shouldHaveSize 1
    messages[0].receiptHandle shouldBe "ack-id"
  }

  @Test
  fun `poll sets a timeout on the pull request`() {
    val context = slot<ApiCallContext>()
    val queue = createQueue(ApiFutures.immediateFuture(PullResponse.getDefaultInstance()), context)

    queue.poll()

    context.captured.timeoutDuration shouldBe Duration.ofSeconds(20)
  }

  @Test
  fun `poll returns empty list on deadline exceeded`() {
    val exception =
        DeadlineExceededException(
            null,
            GrpcStatusCode.of(Status.Code.DEADLINE_EXCEEDED),
            false,
        )
    val queue = createQueue(ApiFutures.immediateFailedFuture(exception))

    queue.poll().shouldBeEmpty()
  }

  @Test
  fun `poll rethrows other exceptions unwrapped`() {
    val exception = NotFoundException(null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false)
    val queue = createQueue(ApiFutures.immediateFailedFuture(exception))

    val thrown = shouldThrow<NotFoundException> { queue.poll() }

    thrown shouldBeSameInstanceAs exception
  }

  @Test
  fun `poll can be interrupted while waiting for messages`() {
    // Never completes, like a pull waiting for messages
    val future = SettableApiFuture.create<PullResponse>()
    val queue = createQueue(future)

    Thread.currentThread().interrupt()
    try {
      shouldThrow<InterruptedException> { queue.poll() }

      future.isCancelled shouldBe true
      // The interrupt flag should be restored, so MessagePoller sees that it should stop
      Thread.currentThread().isInterrupted shouldBe true
    } finally {
      // Clear the interrupt flag, so it doesn't leak into other tests
      Thread.interrupted()
    }
  }

  private fun createQueue(
      response: ApiFuture<PullResponse>,
      capturedContext: CapturingSlot<ApiCallContext> = slot(),
  ): PubSubQueue {
    val pullCallable = mockk<UnaryCallable<PullRequest, PullResponse>>()
    every { pullCallable.futureCall(any(), capture(capturedContext)) } returns response

    val subscriber = mockk<SubscriberStub>()
    every { subscriber.pullCallable() } returns pullCallable

    return PubSubQueue(subscriber, subscriptionName = "test-subscription")
  }
}
