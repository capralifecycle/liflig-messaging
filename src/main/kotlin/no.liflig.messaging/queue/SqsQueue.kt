@file:Suppress("unused") // This is a library

package no.liflig.messaging.queue

import java.time.Duration
import no.liflig.logging.field
import no.liflig.logging.getLogger
import no.liflig.logging.rawJsonField
import no.liflig.messaging.Message
import no.liflig.messaging.backoff.BackoffConfig
import no.liflig.messaging.backoff.SqsBackoffService
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.Message as SQSMessage
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest

private val log = getLogger {}

/**
 * [Queue] implementation for AWS SQS (Simple Queue Service).
 *
 * @param name Used for logging in [send]. Should be human-readable, and have "queue" somewhere in
 *   the name.
 * @param messagesAreValidJson We log outgoing message bodies in [send], and incoming message bodies
 *   in [MessagePoller][no.liflig.messaging.MessagePoller]. We want to log these as raw JSON to
 *   enable log analysis with CloudWatch. But we can't necessarily trust that the body is valid
 *   JSON, because it may originate from some third party - and logging it as raw JSON in that case
 *   would break our logs. But if this is an internal-only queue where we know the bodies are valid
 *   JSON, we can set this flag to true to avoid having to validate the body.
 */
public class SqsQueue(
    private val sqsClient: SqsClient,
    private val queueUrl: String,
    private val name: String = "queue",
    override val messagesAreValidJson: Boolean = false,
    backoffConfig: BackoffConfig = BackoffConfig()
) : Queue {
  private val backoffService = SqsBackoffService(sqsClient, backoffConfig)

  override fun send(
      messageBody: String,
      customAttributes: Map<String, String>,
      systemAttributes: Map<String, String>,
      delay: Duration?
  ) {
    val response =
        try {
          val messageAttributes =
              customAttributes.mapValues { (_, stringValue) ->
                MessageAttributeValue.builder().dataType("String").stringValue(stringValue).build()
              }

          val messageSystemAttributes =
              systemAttributes.mapValues { (_, stringValue) ->
                MessageSystemAttributeValue.builder()
                    .dataType("String")
                    .stringValue(stringValue)
                    .build()
              }

          sqsClient.sendMessage { req ->
            req.queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(messageAttributes)
                .messageSystemAttributesWithStrings(messageSystemAttributes)
            if (delay != null) {
              req.delaySeconds(delay.toSeconds().toInt())
            }
          }
        } catch (e: Exception) {
          throw MessageSendingException(
              "Failed to send message to ${name}",
              cause = e,
              logFields =
                  listOf(
                      rawJsonField(
                          "outgoingQueueMessage",
                          messageBody,
                          validJson = messagesAreValidJson,
                      ),
                      field("queueUrl", queueUrl),
                  ),
          )
        }

    log.info {
      // Add "outgoing" prefix to these log fields, for the cases where this is mapped from an
      // incoming event, and we want fields from both outgoing and incoming to be included
      field("outgoingQueueMessageId", response.messageId())
      rawJsonField("outgoingQueueMessage", messageBody, validJson = messagesAreValidJson)
      field("queueUrl", queueUrl)
      "Sent message to ${name}"
    }
  }

  override fun delete(message: Message) {
    sqsClient.deleteMessage { req ->
      req.queueUrl(queueUrl)
      req.receiptHandle(message.receiptHandle)
    }
  }

  override fun poll(): List<Message> {
    val receiveRequest =
        ReceiveMessageRequest.builder()
            .messageSystemAttributeNames(MessageSystemAttributeName.ALL)
            .messageAttributeNames("All")
            .queueUrl(queueUrl)
            .waitTimeSeconds(20)
            .maxNumberOfMessages(10)
            .build()

    return sqsClient.receiveMessage(receiveRequest).messages().map { it.toInternalFormat() }
  }

  override fun retry(message: Message) {
    backoffService.increaseVisibilityTimeout(message, queueUrl)
  }
}

internal fun SQSMessage.toInternalFormat(): Message {
  val customAttributes =
      if (!this.hasMessageAttributes()) {
        emptyMap()
      } else {
        // Set initial capacity to avoid reallocations
        val map = HashMap<String, String>(this.messageAttributes().size)
        for ((key, value) in this.messageAttributes()) {
          when (value.dataType()) {
            // Both String and Number data types in SQS use the StringValue field
            "String",
            "Number" -> map[key] = value.stringValue()
            // To keep the Message type simple, we omit Binary attributes. If we find a future use
            // case for this, we should expand the Message type with e.g. a `binaryCustomAttributes`
            // field.
            "Binary" -> {}
          }
        }
        map
      }

  return Message(
      id = this.messageId(),
      body = this.body(),
      receiptHandle = this.receiptHandle(),
      systemAttributes = this.attributesAsStrings(),
      customAttributes = customAttributes,
  )
}
