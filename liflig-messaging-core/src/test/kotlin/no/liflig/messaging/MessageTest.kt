package no.liflig.messaging

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import java.time.Instant
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class MessageTest {
  @Test
  fun `getSqsSentTimestamp works as expected`() {
    val sentTimestamp = Instant.parse("2025-03-14T13:54:31Z")
    val message =
        Message(
                id = MessageId("72dc8184-30e5-4575-9834-060b3dd60e7c"),
                body = """{"test":true}""",
                systemAttributes = emptyMap(),
                customAttributes = emptyMap(),
            )
            .setSqsSentTimestamp(sentTimestamp)

    message.systemAttributes["SentTimestamp"].shouldNotBeNull() shouldBe
        sentTimestamp.toEpochMilli().toString()

    message.getSqsSentTimestamp() shouldBe sentTimestamp
  }

  @Test
  fun `getSentTimestamp reads SQS SentTimestamp`() {
    val sentTimestamp = Instant.parse("2025-03-14T13:54:31Z")
    val message = testMessage(systemAttributes = emptyMap()).setSqsSentTimestamp(sentTimestamp)

    message.getSentTimestamp() shouldBe sentTimestamp
  }

  @Test
  fun `getSentTimestamp reads Pub-Sub PublishTime`() {
    val message = testMessage(systemAttributes = mapOf("PublishTime" to "1700000000500"))

    message.getSentTimestamp() shouldBe Instant.ofEpochMilli(1_700_000_000_500)
  }

  @Test
  fun `getSentTimestamp throws when no timestamp attribute is present`() {
    val message = testMessage(systemAttributes = emptyMap())

    shouldThrow<IllegalStateException> { message.getSentTimestamp() }
  }

  private fun testMessage(systemAttributes: Map<String, String>) =
      Message(
          id = MessageId("72dc8184-30e5-4575-9834-060b3dd60e7c"),
          body = """{"test":true}""",
          systemAttributes = systemAttributes,
          customAttributes = emptyMap(),
      )
}
