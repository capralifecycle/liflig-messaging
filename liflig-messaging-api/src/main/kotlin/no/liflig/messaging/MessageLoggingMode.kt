package no.liflig.messaging

import no.liflig.logging.LogBuilder
import no.liflig.logging.LogField
import no.liflig.logging.field
import no.liflig.logging.rawJsonField

public enum class MessageLoggingMode {
  DISABLED,
  JSON,
  VALID_JSON,
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
