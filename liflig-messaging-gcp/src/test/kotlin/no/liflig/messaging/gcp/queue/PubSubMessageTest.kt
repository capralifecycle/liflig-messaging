package no.liflig.messaging.gcp.queue

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.ReceivedMessage
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.maps.shouldNotContainKey
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.opentelemetry.api.trace.Span
import org.junit.jupiter.api.Test

internal class PubSubMessageTest {
  @Test
  fun `maps publish time to milliseconds`() {
    val received =
        buildReceivedMessage(
            publishTime =
                Timestamp.newBuilder().setSeconds(1_700_000_000).setNanos(500_000_000).build(),
        )

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.systemAttributes[PubSubQueue.PUBLISH_TIME_ATTRIBUTE] shouldBe "1700000000500"
  }

  @Test
  fun `omits delivery attempt when zero`() {
    val received = buildReceivedMessage(deliveryAttempt = 0)

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.systemAttributes.shouldNotContainKey(PubSubQueue.DELIVERY_ATTEMPT_ATTRIBUTE)
  }

  @Test
  fun `includes delivery attempt when set`() {
    val received = buildReceivedMessage(deliveryAttempt = 3)

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.systemAttributes[PubSubQueue.DELIVERY_ATTEMPT_ATTRIBUTE] shouldBe "3"
  }

  @Test
  fun `omits ordering key when empty`() {
    val received = buildReceivedMessage(orderingKey = "")

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.systemAttributes.shouldNotContainKey(PubSubQueue.ORDERING_KEY_ATTRIBUTE)
  }

  @Test
  fun `includes ordering key when set`() {
    val received = buildReceivedMessage(orderingKey = "order-123")

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.systemAttributes[PubSubQueue.ORDERING_KEY_ATTRIBUTE] shouldBe "order-123"
  }

  @Test
  fun `maps custom attributes, body, ack ID and source`() {
    val received =
        buildReceivedMessage(
            customAttributes = mapOf("eventType" to "OrderCreated"),
            body = """{"orderId":"123"}""",
            ackId = "ack-id-1",
        )

    val message = pubsubMessageToInternalFormat(received, source = "test-subscription")

    message.customAttributes shouldContainExactly mapOf("eventType" to "OrderCreated")
    message.body shouldBe """{"orderId":"123"}"""
    message.receiptHandle shouldBe "ack-id-1"
    message.source shouldBe "test-subscription"
  }

  @Test
  fun `excludes googclient_ prefixed attributes from customAttributes`() {
    val received =
        buildReceivedMessage(
            customAttributes =
                mapOf(
                    "eventType" to "OrderCreated",
                    "googclient_traceparent" to "00-trace-id-01",
                ),
        )

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.customAttributes shouldContainExactly mapOf("eventType" to "OrderCreated")
  }

  @Test
  fun `context is null when no trace context attribute is present`() {
    val received = buildReceivedMessage()

    val message = pubsubMessageToInternalFormat(received, source = "test")

    message.context.shouldBeNull()
  }

  @Test
  fun `context is extracted when traceparent attribute is present`() {
    val received =
        buildReceivedMessage(
            customAttributes =
                mapOf(
                    "googclient_traceparent" to
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                    "googclient_tracestate" to "vendor=value",
                ),
        )

    val message = pubsubMessageToInternalFormat(received, source = "test")

    val spanContext = Span.fromContext(message.context.shouldNotBeNull()).spanContext
    spanContext.isValid shouldBe true
    spanContext.isRemote shouldBe true
    spanContext.isSampled shouldBe true
    spanContext.traceId shouldBe "4bf92f3577b34da6a3ce929d0e0e4736"
    spanContext.spanId shouldBe "00f067aa0ba902b7"
    spanContext.traceState.get("vendor") shouldBe "value"
  }

  private fun buildReceivedMessage(
      publishTime: Timestamp = Timestamp.newBuilder().setSeconds(1_700_000_000).build(),
      deliveryAttempt: Int = 0,
      orderingKey: String = "",
      customAttributes: Map<String, String> = emptyMap(),
      body: String = "test-body",
      ackId: String = "test-ack-id",
  ): ReceivedMessage {
    val pubsubMessage =
        PubsubMessage.newBuilder()
            .setMessageId("test-message-id")
            .setData(ByteString.copyFromUtf8(body))
            .setPublishTime(publishTime)
            .setOrderingKey(orderingKey)
            .putAllAttributes(customAttributes)
            .build()

    return ReceivedMessage.newBuilder()
        .setAckId(ackId)
        .setMessage(pubsubMessage)
        .setDeliveryAttempt(deliveryAttempt)
        .build()
  }
}
