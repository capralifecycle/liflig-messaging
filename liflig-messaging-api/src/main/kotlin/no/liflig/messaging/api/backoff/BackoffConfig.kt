package no.liflig.messaging.api.backoff

/** A config used for configuring backoff from a queue */
public data class BackoffConfig(
    /** The maximum amount of time a backoff will last. Defaults to 20 min. */
    val maxTimeOutMinutes: Int = MAX_TIMEOUT_MINUTES_DEFAULT,
    /**
     * The factor applied on each backoff. Defaults to 2.0, which means the backoff time doubles on
     * each backoff.
     */
    val backoffFactor: Double = BACKOFF_FACTOR_DEFAULT,
    /** The initial backoff interval. Defaults to 30 seconds. */
    val initialIntervalSeconds: Int = INITIAL_INTERVAL_SECONDS_DEFAULT
) {
  public companion object {
    private const val MAX_TIMEOUT_MINUTES_DEFAULT = 20
    private const val BACKOFF_FACTOR_DEFAULT = 2.0
    private const val INITIAL_INTERVAL_SECONDS_DEFAULT = 30
  }
}
