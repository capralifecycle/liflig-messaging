package no.liflig.messaging.observability

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context
import no.liflig.messaging.Message
import no.liflig.messaging.MessagePollerObserver
import no.liflig.messaging.ProcessingResult

/**
 * Pulls OpenTelemetry context from polled messages and wraps message processing in an appropriate
 * span.
 */
public class OpenTelemetryMessagePollerObserver(
    private val queueName: String,
    private val pollerName: String,
) : MessagePollerObserver {
  private val destinationNameKey = AttributeKey.stringKey("messaging.destination.name")
  private val messageIdKey = AttributeKey.stringKey("messaging.message.id")
  private val operationNameKey = AttributeKey.stringKey("messaging.operation.name")
  private val operationTypeKey = AttributeKey.stringKey("messaging.operation.type")
  private val clientKey = AttributeKey.stringKey("messaging.client.id")

  private val tracer = GlobalOpenTelemetry.getTracer("liflig-messaging")

  override fun onMessageSuccess(message: Message) {
    Span.current().setStatus(StatusCode.OK)
  }

  override fun onMessageFailure(message: Message, result: ProcessingResult.Failure) {
    val span = Span.current()
    span.setStatus(StatusCode.ERROR)
    result.cause?.let { span.recordException(it) }
  }

  override fun <ReturnT> wrapMessageProcessing(
      message: Message,
      messageProcessingBlock: () -> ReturnT,
  ): ReturnT {
    val context = message.context ?: Context.root()
    val span =
        tracer
            .spanBuilder("process $queueName")
            .setParent(context)
            .setSpanKind(SpanKind.CONSUMER)
            .setAttribute(destinationNameKey, queueName)
            .setAttribute(messageIdKey, message.id.value)
            .setAttribute(operationNameKey, "process")
            .setAttribute(operationTypeKey, "process")
            .setAttribute(clientKey, pollerName)
            .startSpan()

    try {
      return span.makeCurrent().use { messageProcessingBlock() }
    } finally {
      span.end()
    }
  }
}
