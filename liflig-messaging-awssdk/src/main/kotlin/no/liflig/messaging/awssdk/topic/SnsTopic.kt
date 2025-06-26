@file:Suppress("unused") // This is a library

package no.liflig.messaging.awssdk.topic

import no.liflig.logging.getLogger
import no.liflig.messaging.MessageId
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.topic.DefaultTopicObserver
import no.liflig.messaging.topic.Topic
import no.liflig.messaging.topic.TopicObserver
import software.amazon.awssdk.services.sns.SnsClient

/**
 * [Topic] implementation for AWS SNS (Simple Notification Service).
 *
 * The class provides multiple constructors:
 * - The primary constructor uses a provided
 *   [TopicObserver][no.liflig.messaging.topic.TopicObserver]
 * - A second utility constructor constructs a
 *   [DefaultTopicObserver][no.liflig.messaging.topic.DefaultTopicObserver] with the given `name`
 *   and [MessageLoggingMode][no.liflig.messaging.MessageLoggingMode]
 */
public class SnsTopic(
    private val snsClient: SnsClient,
    private val topicArn: String,
    private val observer: TopicObserver,
) : Topic {
  public constructor(
      snsClient: SnsClient,
      topicArn: String,
      name: String = "topic",
      loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
  ) : this(
      snsClient,
      topicArn,
      observer = DefaultTopicObserver(topicName = name, topicArn = topicArn, logger, loggingMode),
  )

  override fun publish(message: String): MessageId {
    val response =
        try {
          snsClient.publish { req -> req.topicArn(topicArn).message(message) }
        } catch (e: Exception) {
          observer.onPublishException(e, message)
        }

    observer.onPublishSuccess(messageId = response.messageId(), messageBody = message)
    return MessageId(response.messageId())
  }

  internal companion object {
    internal val logger = getLogger()
  }
}
