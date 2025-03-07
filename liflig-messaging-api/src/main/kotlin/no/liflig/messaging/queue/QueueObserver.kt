package no.liflig.messaging.queue

import no.liflig.logging.LogField
import no.liflig.logging.Logger
import no.liflig.logging.field
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.addMessageBodyToLog
import no.liflig.messaging.messageBodyLogField

public interface QueueObserver {
  public fun onSendSuccess(messageId: String, messageBody: String)

  public fun onSendException(exception: Throwable, messageBody: String): Nothing
}

public open class DefaultQueueObserver(
    private val queueName: String = "queue",
    private val queueUrl: String? = null,
    private val logger: Logger = Queue.logger,
    private val loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
) : QueueObserver {
  override fun onSendSuccess(messageId: String, messageBody: String) {
    logger.info {
      // Add "outgoing" prefix to these log fields, for the cases where this is mapped from an
      // incoming event, and we want fields from both outgoing and incoming to be included
      field("outgoingQueueMessageId", messageId)
      addMessageBodyToLog("outgoingQueueMessage", messageBody, loggingMode)
      if (queueUrl != null) {
        field("queueUrl", queueUrl)
      }
      "Sent message to ${queueName}"
    }
  }

  override fun onSendException(exception: Throwable, messageBody: String): Nothing {
    val logFields: List<LogField> =
        buildList(2) {
          add(field("queueUrl", queueUrl))
          val bodyField = messageBodyLogField("outgoingQueueMessage", messageBody, loggingMode)
          if (bodyField != null) {
            add(bodyField)
          }
        }

    throw MessageSendingException(
        "Failed to send message to ${queueName}",
        cause = exception,
        logFields,
    )
  }
}
