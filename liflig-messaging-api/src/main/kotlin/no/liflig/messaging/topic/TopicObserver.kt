package no.liflig.messaging.topic

import no.liflig.logging.LogField
import no.liflig.logging.Logger
import no.liflig.logging.field
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.addMessageBodyToLog
import no.liflig.messaging.messageBodyLogField

public interface TopicObserver {
  public fun onPublishSuccess(messageId: String, messageBody: String)

  public fun onPublishException(exception: Throwable, messageBody: String): Nothing
}

public open class DefaultTopicObserver(
    private val topicName: String = "topic",
    private val topicArn: String? = null,
    private val logger: Logger = Topic.logger,
    private val loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
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
        buildList(2) {
          val bodyField = messageBodyLogField("publishedMessage", messageBody, loggingMode)
          if (bodyField != null) {
            add(bodyField)
          }

          if (topicArn != null) {
            add(field("topicArn", topicArn))
          }
        }

    throw TopicPublishException(
        "Failed to publish message to ${topicName}",
        cause = exception,
        logFields,
    )
  }
}
