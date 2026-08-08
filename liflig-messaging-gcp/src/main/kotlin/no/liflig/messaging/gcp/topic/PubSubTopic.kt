@file:Suppress("unused") // This is a library

package no.liflig.messaging.gcp.topic

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import no.liflig.logging.getLogger
import no.liflig.messaging.MessageId
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.topic.DefaultTopicObserver
import no.liflig.messaging.topic.Topic
import no.liflig.messaging.topic.TopicObserver

/**
 * [Topic] implementation for Google Cloud Pub/Sub.
 *
 * Wraps a Pub/Sub [Publisher], which is configured with the topic to publish to (and how to connect
 * to it). You're responsible for the lifecycle of the [Publisher]: when your application shuts
 * down, you should call [Publisher.shutdown] (and optionally [Publisher.awaitTermination]).
 *
 * The class provides multiple constructors:
 * - The primary constructor uses a provided
 *   [TopicObserver][no.liflig.messaging.topic.TopicObserver]
 * - A second utility constructor constructs a
 *   [DefaultTopicObserver][no.liflig.messaging.topic.DefaultTopicObserver] with the given `name`
 *   and [MessageLoggingMode][no.liflig.messaging.MessageLoggingMode]
 */
public class PubSubTopic(
    private val publisher: Publisher,
    private val observer: TopicObserver,
) : Topic {
  public constructor(
      publisher: Publisher,
      name: String = "topic",
      loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
  ) : this(
      publisher,
      observer =
          DefaultTopicObserver(
              topicName = name,
              topicArn = publisher.topicNameString,
              logger,
              loggingMode,
          ),
  )

  override fun publish(message: String): MessageId {
    val messageId =
        try {
          val pubsubMessage =
              PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build()
          // publish returns an ApiFuture that completes once the message is sent. We block on it to
          // get the assigned message ID (and to surface any publishing error synchronously).
          publisher.publish(pubsubMessage).get()
        } catch (e: Exception) {
          observer.onPublishException(e, message)
        }

    observer.onPublishSuccess(messageId = messageId, messageBody = message)
    return MessageId(messageId)
  }

  internal companion object {
    internal val logger = getLogger()
  }
}
