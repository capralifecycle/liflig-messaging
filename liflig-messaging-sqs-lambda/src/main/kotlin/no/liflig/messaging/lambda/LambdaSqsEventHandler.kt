package no.liflig.messaging.lambda

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import no.liflig.logging.field
import no.liflig.logging.getLogger
import no.liflig.logging.rawJsonField
import no.liflig.logging.withLoggingContext
import no.liflig.messaging.api.DefaultMessagePollerObserver
import no.liflig.messaging.api.Message
import no.liflig.messaging.api.MessagePollerObserver
import no.liflig.messaging.api.MessageProcessor
import no.liflig.messaging.api.ProcessingResult

private val log = getLogger {}

/**
 * Iterates over queue messages in the given AWS Lambda SQS event, and passes them to the given
 * [MessageProcessor][no.liflig.messaging.MessageProcessor]. This function handles failures
 * gracefully, gathering failed messages into an
 * [SQSBatchResponse][com.amazonaws.services.lambda.runtime.events.SQSBatchResponse] and returning
 * it. This response must be returned by your Lambda handler.
 *
 * NOTE: Returning an `SQSBatchResponse` only works if you've enabled `reportBatchItemFailures` on
 * your Lambda <-> SQS integration. In AWS CDK, you do this on the `SqsEventSource`:
 * ```ts
 * myLambda.addEventSource(
 *   new SqsEventSource(myQueue, {reportBatchItemFailures: true}),
 * )
 * ```
 *
 * The reason for doing this is to avoid duplicate message processing. The Lambda <-> SQS
 * integration gathers messages into a batch before invoking the lambda (that's why
 * [SQSEvent.getRecords][com.amazonaws.services.lambda.runtime.events.SQSEvent.getRecords] returns a
 * list). If the lambda throws an exception for one of the messages, all of them will be retried.
 * This means that if message 1 was successfully processed, and message 2 throws an exception, then
 * both message 1 and 2 will be retried. If the lambda produces outgoing events, this will lead to
 * events being produced twice. An event-driven system should be idempotent, but we would still like
 * to avoid unnecessary duplicate events. `reportBatchItemFailures` solves this problem, by
 * returning an `SQSBatchResponse` back to the Lambda runtime that signals which specific messages
 * failed to process.
 *
 * ### Example
 *
 * ```
 * class LambdaHandler(
 *     private val messageProcessor: MessageProcessor = ExampleMessageProcessor(),
 * ) {
 *   /** Method called by AWS Lambda (configured in our infra code). */
 *   fun handle(sqsEvent: SQSEvent): SQSBatchResponse {
 *     return handleLambdaSqsEvent(sqsEvent, exampleMessageProcessor)
 *   }
 * }
 * ```
 *
 * @param messagesAreValidJson We want to log incoming messages as raw JSON, to enable log analysis
 *   with CloudWatch. But we can't necessarily trust that the body is valid JSON, because it may
 *   originate from some third party - and logging it as raw JSON in that case would break our logs.
 *   So by default, we validate that the message body is actually valid JSON. But if this is an
 *   internal-only queue where you know the messages are valid JSON, you can set this flag to true
 *   to avoid the cost of validating the body.
 */
public fun handleLambdaSqsEvent(
    sqsEvent: SQSEvent,
    messageProcessor: MessageProcessor,
    messagesAreValidJson: Boolean = false,
    observer: MessagePollerObserver = DefaultMessagePollerObserver(pollerName = null, logger = log),
): SQSBatchResponse {
  val messages = sqsEvent.records.map(::lambdaSqsMessageToInternalFormat)
  val failedMessages = mutableListOf<BatchItemFailure>()

  observer.onPoll(messages)

  for (message in messages) {
    withLoggingContext(
        field("queueMessageId", message.id),
        rawJsonField("queueMessage", message.body, validJson = messagesAreValidJson),
    ) {
      try {
        observer.onMessageProcessing(message)

        when (val result = messageProcessor.process(message)) {
          is ProcessingResult.Success -> {
            observer.onMessageSuccess(message)
            // Do nothing here - not adding the message to failedMessages means it will be deleted
          }
          is ProcessingResult.Failure -> {
            observer.onMessageFailure(message, result)
            if (result.retry) {
              failedMessages.add(BatchItemFailure(message.id))
            } else {
              // Do nothing here - not adding the message to failedMessages means it will be deleted
            }
          }
        }
      } catch (e: Exception) {
        observer.onMessageException(message, e)
        failedMessages.add(BatchItemFailure(message.id))
      }
    }
  }

  return SQSBatchResponse(failedMessages)
}

internal fun lambdaSqsMessageToInternalFormat(sqsMessage: SQSEvent.SQSMessage): Message {
  val customAttributes: Map<String, String> =
      if (sqsMessage.messageAttributes == null) {
        emptyMap()
      } else {
        // Set initial capacity to avoid reallocations
        val map = HashMap<String, String>(sqsMessage.messageAttributes.size)
        for ((key, value) in sqsMessage.messageAttributes) {
          when (value.dataType) {
            // Both String and Number data types in SQS use the StringValue field
            "String",
            "Number" -> map[key] = value.stringValue
            // To keep the Message type simple, we omit Binary attributes. If we find a future use
            // case for this, we should expand the Message type with e.g. a `binaryCustomAttributes`
            // field.
            "Binary" -> {}
          }
        }
        map
      }

  return Message(
      id = sqsMessage.messageId,
      body = sqsMessage.body,
      receiptHandle = sqsMessage.receiptHandle,
      systemAttributes = sqsMessage.attributes ?: emptyMap(),
      customAttributes = customAttributes,
      source = sqsMessage.eventSourceArn,
  )
}
