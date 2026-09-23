package no.liflig.messaging.gcp.backoff

import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.Empty
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import no.liflig.messaging.Message
import no.liflig.messaging.MessageId
import no.liflig.messaging.backoff.BackoffConfig
import no.liflig.messaging.gcp.queue.PubSubQueue
import org.junit.jupiter.api.Test

internal class PubSubBackoffServiceTest {
  @Test
  fun `sets ack deadline on the given subscription and ack ID`() {
    val request = increaseVisibilityTimeout(deliveryAttempt = null)

    request.subscription shouldBe "projects/test-project/subscriptions/test-subscription"
    request.ackIdsList shouldContainExactly listOf("test-ack-id")
  }

  @Test
  fun `uses initial interval when delivery attempt is missing`() {
    // Pub/Sub only populates the delivery attempt when the subscription has a dead-letter policy
    val request = increaseVisibilityTimeout(deliveryAttempt = null)

    request.ackDeadlineSeconds shouldBe 30
  }

  @Test
  fun `backs off exponentially based on delivery attempt`() {
    val request = increaseVisibilityTimeout(deliveryAttempt = 3)

    // 30 seconds * 2^(3 - 1)
    request.ackDeadlineSeconds shouldBe 120
  }

  @Test
  fun `clamps ack deadline to Pub-Sub's maximum`() {
    // 30 seconds * 2^(6 - 1) = 960 seconds, which is below the 20 minute max timeout in
    // BackoffConfig, but above Pub/Sub's max ack deadline of 600 seconds
    val request = increaseVisibilityTimeout(deliveryAttempt = 6)

    request.ackDeadlineSeconds shouldBe PubSubBackoffService.MAX_ACK_DEADLINE_SECONDS
  }

  private fun increaseVisibilityTimeout(deliveryAttempt: Int?): ModifyAckDeadlineRequest {
    val request = slot<ModifyAckDeadlineRequest>()
    val modifyAckDeadlineCallable = mockk<UnaryCallable<ModifyAckDeadlineRequest, Empty>>()
    every { modifyAckDeadlineCallable.call(capture(request)) } returns Empty.getDefaultInstance()
    val subscriber = mockk<SubscriberStub>()
    every { subscriber.modifyAckDeadlineCallable() } returns modifyAckDeadlineCallable

    val message =
        Message(
            id = MessageId("test-message-id"),
            body = "test-body",
            systemAttributes =
                if (deliveryAttempt == null) {
                  emptyMap()
                } else {
                  mapOf(PubSubQueue.DELIVERY_ATTEMPT_ATTRIBUTE to deliveryAttempt.toString())
                },
            customAttributes = emptyMap(),
            receiptHandle = "test-ack-id",
        )

    PubSubBackoffService(subscriber, BackoffConfig())
        .increaseVisibilityTimeout(
            message,
            queueUrl = "projects/test-project/subscriptions/test-subscription",
        )

    return request.captured
  }
}
