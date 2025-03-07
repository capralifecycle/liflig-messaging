package no.liflig.messaging.queue

import no.liflig.logging.LogField
import no.liflig.logging.Logger
import no.liflig.logging.field
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.addMessageBodyToLog
import no.liflig.messaging.messageBodyLogField
import no.liflig.messaging.topic.Topic
import no.liflig.messaging.topic.TopicObserver

/**
 * Interface for observing various events in a [Queue]'s methods.
 *
 * A default implementation is provided by [DefaultQueueObserver], which uses `liflig-logging` to
 * log descriptive messages for these events.
 */
public interface QueueObserver {
  /** Called after a message is sent to the queue. */
  public fun onSendSuccess(messageId: String, messageBody: String)

  /**
   * Called when an exception is thrown when trying to send a message to the queue.
   *
   * This method returns [Nothing], so it _must_ throw. You can either re-throw the given exception,
   * or wrap it in another exception to provide extra context.
   * [DefaultQueueObserver.onSendException] wraps the exception in [MessageSendingException].
   */
  public fun onSendException(exception: Throwable, messageBody: String): Nothing
}

/**
 * Default implementation of [TopicObserver], using `liflig-logging` to log descriptive messages for
 * the various events in [Topic]'s methods.
 *
 * @param queueName Included in logs when publishing messages. Should be human-readable, and have
 *   "queue" somewhere in the name.
 * @param logger Defaults to [Queue]'s logger, so the logger name will show as:
 *   `no.liflig.messaging.queue.Queue`. If you want a different logger name, you can construct your
 *   own logger (using [no.liflig.logging.getLogger]) and pass it here.
 * @param loggingMode Controls how message bodies are logged. Defaults to
 *   [MessageLoggingMode.JSON][no.liflig.messaging.MessageLoggingMode.JSON], which tries to include
 *   the message as raw JSON, but checks that it's valid JSON first.
 */
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
        buildList(capacity = 2) {
          val bodyField = messageBodyLogField("outgoingQueueMessage", messageBody, loggingMode)
          if (bodyField != null) {
            add(bodyField)
          }

          if (queueUrl != null) {
            add(field("queueUrl", queueUrl))
          }
        }

    throw MessageSendingException(
        "Failed to send message to ${queueName}",
        cause = exception,
        logFields,
    )
  }
}
