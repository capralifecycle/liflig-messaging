package no.liflig.messaging

import no.liflig.logging.LogBuilder
import no.liflig.logging.LogField
import no.liflig.logging.field
import no.liflig.logging.rawJsonField

/**
 * The various default observer classes ([DefaultMessagePollerObserver],
 * [DefaultQueueObserver][no.liflig.messaging.queue.DefaultQueueObserver],
 * [DefaultTopicObserver][no.liflig.messaging.topic.DefaultTopicObserver]) take this enum as an
 * argument to control how message bodies are logged.
 */
public enum class MessageLoggingMode {
  /** Don't log message bodies (use this if messages may contain sensitive data). */
  DISABLED,
  /**
   * Tries to include the message body as raw JSON on the log (to enable structured log analysis),
   * but checks that it's valid JSON first so we don't break our logs.
   *
   * Use this if the messages originate from an external system, and they should be JSON, but you
   * can't trust that 100%. If these messages are from an internal system that you fully trust, you
   * can use [MessageLoggingMode.VALID_JSON] instead.
   */
  JSON,
  /**
   * Includes the message body as raw JSON on the log (to enable structured log analysis). Assumes
   * that the messages are known to be valid JSON, so we can avoid the cost of validating them.
   *
   * Use this if the messages originate from an internal system, and you know 100% that they're
   * valid JSON. If you can't trust this, you should use [MessageLoggingMode.JSON] instead.
   */
  VALID_JSON,
  /**
   * Includes the message body as a string field on the log. Use this if the messages are not JSON,
   * but you still want to include them in the logs.
   */
  NOT_JSON,
}

internal fun LogBuilder.addMessageBodyToLog(
    key: String,
    messageBody: String,
    mode: MessageLoggingMode
) {
  when (mode) {
    MessageLoggingMode.DISABLED -> {}
    MessageLoggingMode.JSON -> this.rawJsonField(key, messageBody, validJson = false)
    MessageLoggingMode.VALID_JSON -> this.rawJsonField(key, messageBody, validJson = true)
    MessageLoggingMode.NOT_JSON -> this.field(key, messageBody)
  }
}

internal fun messageBodyLogField(
    key: String,
    messageBody: String,
    mode: MessageLoggingMode
): LogField? {
  return when (mode) {
    MessageLoggingMode.DISABLED -> null
    MessageLoggingMode.JSON -> rawJsonField(key, messageBody, validJson = false)
    MessageLoggingMode.VALID_JSON -> rawJsonField(key, messageBody, validJson = true)
    MessageLoggingMode.NOT_JSON -> field(key, messageBody)
  }
}
