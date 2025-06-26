package no.liflig.messaging.topic

import no.liflig.logging.LogField
import no.liflig.logging.Logger
import no.liflig.logging.field
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.addMessageBodyToLog
import no.liflig.messaging.messageBodyLogField

/**
 * Interface for observing various events in a [Topic]'s methods.
 *
 * A default implementation is provided by [DefaultTopicObserver], which uses `liflig-logging` to
 * log descriptive messages for these events.
 */
public interface TopicObserver {
  /** Called after a message is published to the topic. */
  public fun onPublishSuccess(messageId: String, messageBody: String)

  /**
   * Called when an exception is thrown when trying to publish a message to the topic.
   *
   * This method returns [Nothing], so it _must_ throw. You can either re-throw the given exception,
   * or wrap it in another exception to provide extra context.
   * [DefaultTopicObserver.onPublishException] wraps the exception in [MessagePublishingException].
   */
  public fun onPublishException(exception: Throwable, messageBody: String): Nothing
}

/**
 * Default implementation of [TopicObserver], using `liflig-logging` to log descriptive messages for
 * the various events in [Topic]'s methods.
 *
 * @param topicName Included in logs when publishing messages. Should be human-readable, and have
 *   "topic" somewhere in the name.
 * @param logger Defaults to [Topic]'s logger, so the logger name will show as:
 *   `no.liflig.messaging.topic.Topic`. If you want a different logger name, you can construct your
 *   own logger (using [no.liflig.logging.getLogger]) and pass it here.
 * @param loggingMode Controls how message bodies are logged. Defaults to
 *   [MessageLoggingMode.JSON][no.liflig.messaging.MessageLoggingMode.JSON], which tries to include
 *   the message as raw JSON, but checks that it's valid JSON first.
 */
public open class DefaultTopicObserver(
    protected val topicName: String = "topic",
    protected val topicArn: String? = null,
    protected val logger: Logger = Topic.logger,
    protected val loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
) : TopicObserver {
  override fun onPublishSuccess(messageId: String, messageBody: String) {
    logger.info {
      field("publishedMessageId", messageId)
      addMessageBodyToLog("publishedMessage", messageBody, loggingMode)
      if (topicArn != null) {
        field("topicArn", topicArn)
      }
      "Published message to ${topicName}"
    }
  }

  override fun onPublishException(exception: Throwable, messageBody: String): Nothing {
    val logFields: List<LogField> =
        buildList(capacity = 2) {
          val bodyField = messageBodyLogField("publishedMessage", messageBody, loggingMode)
          if (bodyField != null) {
            add(bodyField)
          }

          if (topicArn != null) {
            add(field("topicArn", topicArn))
          }
        }

    throw MessagePublishingException(
        "Failed to publish message to ${topicName}",
        cause = exception,
        logFields,
    )
  }
}
