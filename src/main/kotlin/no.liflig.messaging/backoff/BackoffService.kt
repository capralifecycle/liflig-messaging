package no.liflig.messaging.backoff

import java.math.BigDecimal
import no.liflig.messaging.Message

internal interface BackoffService {
  fun increaseVisibilityTimeout(message: Message, queueUrl: String)

  companion object {
    @JvmStatic
    fun getNextVisibilityTimeout(
        approximateReceiveCount: Int,
        maxTimeoutMinutes: Int,
        backoffFactor: Double,
        initialIntervalSeconds: Int,
    ): Int {
      // We should not try to calculate factor^veryLargeNumber, BigDecimal throws arithmetic
      // exception if above 999999999
      val retryNumber = (approximateReceiveCount.coerceAtMost(9999) - 1).coerceAtLeast(0)
      var nextVisibilityTimeout =
          BigDecimal(backoffFactor).pow(retryNumber).multiply(BigDecimal(initialIntervalSeconds))
      val maxVisibilityTimeout = BigDecimal(maxTimeoutMinutes * 60)
      if (nextVisibilityTimeout > maxVisibilityTimeout) {
        nextVisibilityTimeout = maxVisibilityTimeout
      }
      return nextVisibilityTimeout.toInt()
    }
  }
}
