package no.liflig.messaging.backoff

import no.liflig.logging.Logger
import no.liflig.messaging.Message

/**
 * Interface for observing events in a [BackoffService]'s methods.
 *
 * A default implementation is provided by [DefaultBackoffServiceObserver], which uses
 * `liflig-logging` to log descriptive messages for these events.
 */
public interface BackoffServiceObserver {
  /** Called when the backoff service increases visibility timeout for a message. */
  public fun onIncreaseVisibilityTimeout(
      message: Message,
      nextVisibilityTimeout: Int,
      approximateReceiveCount: Int,
  )
}

/**
 * Default implementation of [BackoffServiceObserver], using `liflig-logging` to log descriptive
 * messages for events in [BackoffService]'s methods.
 *
 * @param logger Defaults to [BackoffService]'s logger, so the logger name will show as:
 *   `no.liflig.messaging.backoff.BackoffService`. If you want a different logger name, you can
 *   construct your own logger (using [no.liflig.logging.getLogger]) and pass it here.
 */
public open class DefaultBackoffServiceObserver(
    protected val logger: Logger = BackoffService.logger,
) : BackoffServiceObserver {
  override fun onIncreaseVisibilityTimeout(
      message: Message,
      nextVisibilityTimeout: Int,
      approximateReceiveCount: Int,
  ) {
    // We log this at the DEBUG log level, since this is a pretty granular log.
    // This log level will be disabled by most library users, but they can enable it in their
    // Logback config with:
    // <logger name="no.liflig.messaging" level="DEBUG"/>
    logger.debug {
      field("nextVisibilityTimeout", nextVisibilityTimeout)
      field("receiveCount", approximateReceiveCount)
      field("systemAttributes", message.systemAttributes)
      field("receiptHandle", message.receiptHandle)
      "Changing message visibility"
    }
  }
}
