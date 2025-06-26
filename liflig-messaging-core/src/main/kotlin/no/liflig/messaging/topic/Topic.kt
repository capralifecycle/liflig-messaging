package no.liflig.messaging.topic

import no.liflig.logging.ExceptionWithLogFields
import no.liflig.logging.LogField
import no.liflig.logging.getLogger
import no.liflig.messaging.MessageId

/**
 * A message topic (a.k.a. event bus) that producers can publish messages to.
 *
 * This library provides 2 implementations:
 * - `SnsTopic` from the `liflig-messaging-awssdk` module, for AWS SNS (Simple Notification Service)
 * - [MockTopic] for tests
 */
public interface Topic {
  /**
   * @return The ID of the published message. In the SNS implementation, this uses the
   *   [`MessageId` returned by AWS in the response](https://docs.aws.amazon.com/sns/latest/api/API_Publish.html#API_Publish_ResponseElements).
   * @throws MessagePublishingException If we failed to publish the message.
   */
  public fun publish(message: String): MessageId

  public companion object {
    internal val logger = getLogger()
  }
}

/**
 * Wraps an underlying publishing exception with extra context for debugging, such as the message
 * body that failed.
 *
 * @param message Note that this is the exception message, not the published message. The published
 *   message is included in [logFields], logged when the exception is passed to `liflig-logging`.
 * @param logFields See [no.liflig.logging.ExceptionWithLogFields].
 */
public class MessagePublishingException(
    override val message: String,
    override val cause: Throwable?,
    logFields: List<LogField>,
) : ExceptionWithLogFields(logFields)
