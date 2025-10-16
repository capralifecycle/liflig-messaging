package no.liflig.messaging.backoff

import io.kotest.matchers.shouldBe
import no.liflig.messaging.backoff.BackoffService.Companion.getNextVisibilityTimeout
import org.junit.jupiter.api.Test

internal class BackoffServiceTest {
  private val maxTimeoutMinutes = 20
  private val backoffFactor = 2.5
  private val initialIntervalSeconds = 30

  @Test
  fun `getNextVisibilityTimeout should return short timeout on backoff count number 1`() {
    // GIVEN
    val approximateReceiveCount = 1
    // WHEN
    val nextTimeout =
        getNextVisibilityTimeout(
            approximateReceiveCount,
            maxTimeoutMinutes,
            backoffFactor,
            initialIntervalSeconds,
        )
    // THEN
    30 shouldBe nextTimeout
  }

  @Test
  fun `getNextVisibilityTimeout should return long timeout on backoff count number 5`() {
    // GIVEN
    val approximateReceiveCount = 5
    // WHEN
    val nextTimeout =
        getNextVisibilityTimeout(
            approximateReceiveCount,
            maxTimeoutMinutes,
            backoffFactor,
            initialIntervalSeconds,
        )
    // THEN
    1171 shouldBe nextTimeout
  }
}
