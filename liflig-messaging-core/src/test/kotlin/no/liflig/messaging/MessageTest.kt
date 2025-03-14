package no.liflig.messaging

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
                id = "72dc8184-30e5-4575-9834-060b3dd60e7c",
                body = """{"test":true}""",
                systemAttributes = emptyMap(),
                customAttributes = emptyMap(),
            )
            .setSqsSentTimestamp(sentTimestamp)

    message.systemAttributes["SentTimestamp"].shouldNotBeNull() shouldBe
        sentTimestamp.toEpochMilli().toString()

    message.getSqsSentTimestamp() shouldBe sentTimestamp
  }
}
