package no.liflig.messaging.awssdk

import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.maps.shouldNotContainKey
import io.kotest.matchers.shouldBe
import no.liflig.messaging.Message
import no.liflig.messaging.awssdk.queue.sqsMessageToInternalFormat
import no.liflig.messaging.awssdk.testutils.readResourceFile
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.sqs.model.Message as SqsMessage
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName

internal class MessageTest {
  @Test
  internal fun testValidAttributes() {
    val message = createSampleMessage()

    val receiveCountKey = MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString()
    message.systemAttributes.shouldContainKey(receiveCountKey)
    message.systemAttributes[receiveCountKey] shouldBe "1"
  }

  @Test
  internal fun testInvalidAttributes() {
    val attr =
        mapOf(
            "ApproximateReceiveeCount" to "1",
            "SentTimestamp" to "1587379397385",
            "SenderId" to "AIDA5ONR45AYSCULMW46P",
            "ApproximateFirstReceiveTimestamp" to "1587379397405",
        )

    val message = createSampleMessage(attributes = attr)
    message.systemAttributes.shouldNotContainKey(
        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString(),
    )
  }

  private fun createSampleMessage(attributes: Map<String, String>? = null): Message {
    val attr =
        attributes
            ?: mapOf(
                "ApproximateReceiveCount" to "1",
                "SentTimestamp" to "1587379397385",
                "SenderId" to "AIDA5ONR45AYSCULMW46P",
                "ApproximateFirstReceiveTimestamp" to "1587379397405",
            )

    val sqsMessage =
        SqsMessage.builder()
            .messageId("bb3ec67e-7cd1-4c46-8218-f5a7821c4157")
            .receiptHandle(
                "AQEB0a80neddLzbvFCg3tmVubDaj6wxxogHGj8IV/bgnRWM6/NpiqLpChv+5YQFzkfHc9AakgN1nnrxBws3F9+UqTN40Ayr7BgYPFFXglEx" +
                    "AxBLnERxBsPi8nBRKIsNanJdV1ID20NeuXZv1ptrinMVtIe0/0aqdxG5L28kCg1BVTNzQS107kP7bshWTf+F+wj8qxKSnNfp87N9Kr+k9" +
                    "t2L/xY2gl9x+0cOQApzR9IC+QZTAOJPW6DT1tX1p8jokRw+cPzwBPHn5jMAK6wbwQ3WV6NkQ0Mk2cBl5EN2xQrCMR6t7KkPnU+qLqN3m+" +
                    "XTi1AKIStKLS5ODju2PlcAcKxtjSXDM3bHPuEw70eCKO/GdR7irWY3j3yN+mmVGywPVHJRr",
            )
            .body(readResourceFile("TestMessage.json"))
            .md5OfBody("822dd494b3e14a82aa76bd455e6b6f4b")
            .attributesWithStrings(attr)
            .md5OfMessageAttributes(null)
            .build()

    return sqsMessageToInternalFormat(sqsMessage)
  }
}
