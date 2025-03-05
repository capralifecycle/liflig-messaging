package no.liflig.messaging.topic

import no.liflig.logging.ExceptionWithLogFields
import no.liflig.logging.LogField

/**
 * A message topic (a.k.a. event bus) that producers can publish messages to.
 *
 * This library provides 2 implementations:
 * - [SnsTopic] for AWS SNS (Simple Notification Service)
 * - [MockTopic] for tests
 */
public interface Topic {
  /** @throws TopicPublishException If we failed to publish the message. */
  public fun publish(message: String)
}

/**
 * Wraps an underlying publishing exception with extra context for debugging, such as the message
 * body that failed.
 *
 * @param message Note that this is the exception message, not the published message. The published
 *   message is included in [logFields], logged when the exception is passed to `liflig-logging`.
 * @param logFields See [no.liflig.logging.ExceptionWithLogFields].
 */
public class TopicPublishException
internal constructor(
    override val message: String,
    override val cause: Exception,
    logFields: List<LogField>,
) : ExceptionWithLogFields()
