@file:Suppress("unused") // This is a library

package no.liflig.messaging.topic

import no.liflig.logging.field
import no.liflig.logging.getLogger
import no.liflig.logging.rawJsonField
import software.amazon.awssdk.services.sns.SnsClient

private val log = getLogger {}

/**
 * [Topic] implementation for AWS SNS (Simple Notification Service).
 *
 * @param name Used for logging when publishing messages. Should be human-readable, and have "topic"
 *   somewhere in the name.
 * @param messagesAreValidJson We log the message in [publish], and want to log it as raw JSON to
 *   enable log analysis with CloudWatch. But we can't necessarily trust that the body is valid
 *   JSON, if it originates from some third party - and logging it as raw JSON in that case would
 *   break our logs. But if we know that the messages will always be valid JSON, we can set this
 *   flag to true to avoid having to validate the message.
 */
public class SnsTopic(
    private val snsClient: SnsClient,
    private val topicArn: String,
    private val name: String = "topic",
    private val messagesAreValidJson: Boolean = false,
) : Topic {
  override fun publish(message: String) {
    val response =
        try {
          snsClient.publish { req -> req.topicArn(topicArn).message(message) }
        } catch (e: Exception) {
          throw TopicPublishException(
              "Failed to publish message to ${name}",
              cause = e,
              logFields =
                  listOf(
                      rawJsonField("publishedMessage", message, validJson = messagesAreValidJson),
                      field("topicArn", topicArn),
                  ),
          )
        }

    log.info {
      field("publishedMessageId", response.messageId())
      rawJsonField("publishedMessage", message, validJson = messagesAreValidJson)
      field("topicArn", topicArn)
      "Published message to ${name}"
    }
  }
}
