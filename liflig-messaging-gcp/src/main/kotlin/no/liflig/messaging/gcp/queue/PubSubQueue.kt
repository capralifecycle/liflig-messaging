@file:Suppress("unused") // This is a library

package no.liflig.messaging.gcp.queue

import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.ByteString
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.ReceivedMessage
import java.time.Duration
import no.liflig.logging.getLogger
import no.liflig.messaging.Message
import no.liflig.messaging.MessageId
import no.liflig.messaging.MessageLoggingMode
import no.liflig.messaging.backoff.BackoffConfig
import no.liflig.messaging.backoff.BackoffService
import no.liflig.messaging.gcp.backoff.PubSubBackoffService
import no.liflig.messaging.queue.DefaultQueueObserver
import no.liflig.messaging.queue.Queue
import no.liflig.messaging.queue.QueueObserver

/**
 * [Queue] implementation for Google Cloud Pub/Sub.
 *
 * Where AWS SQS exposes a single resource that you both send to and poll from, Pub/Sub splits
 * these: you publish to a _topic_, and consume from a _subscription_ attached to that topic. This
 * class therefore wraps:
 * - a [SubscriberStub] + `subscriptionName`, used for [poll], [delete] and [retry], and
 * - an optional [Publisher], used for [send]. Many consumers only poll their queue, so the
 *   publisher may be omitted; calling [send] without one throws [IllegalStateException]. When
 *   provided, the publisher should target the topic that `subscriptionName` is subscribed to, so
 *   that sent messages come back around on [poll].
 *
 * You own the lifecycle of the [SubscriberStub] and [Publisher]: close/shut them down when your
 * application stops.
 *
 * The class provides multiple constructors:
 * - The primary constructor uses a provided
 *   [QueueObserver][no.liflig.messaging.queue.QueueObserver] and
 *   [BackoffService][no.liflig.messaging.backoff.BackoffService]
 * - A second utility constructor constructs a
 *   [DefaultQueueObserver][no.liflig.messaging.queue.DefaultQueueObserver] with the given `name`
 *   and [MessageLoggingMode][no.liflig.messaging.MessageLoggingMode], and a default
 *   [BackoffService][no.liflig.messaging.backoff.BackoffService] implementation using the given
 *   [BackoffConfig][no.liflig.messaging.backoff.BackoffConfig]
 */
public class PubSubQueue(
    private val subscriber: SubscriberStub,
    private val subscriptionName: String,
    private val publisher: Publisher? = null,
    override val observer: QueueObserver,
    private val backoffService: BackoffService,
) : Queue {
  public constructor(
      subscriber: SubscriberStub,
      subscriptionName: String,
      publisher: Publisher? = null,
      name: String = "queue",
      loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
      backoffConfig: BackoffConfig = BackoffConfig(),
  ) : this(
      subscriber,
      subscriptionName,
      publisher,
      observer =
          DefaultQueueObserver(
              queueName = name,
              queueUrl = subscriptionName,
              logger,
              loggingMode,
          ),
      backoffService = PubSubBackoffService(subscriber, backoffConfig),
  )

  /**
   * Publishes a message to the topic backing this queue's subscription.
   *
   * @param delay Ignored. Pub/Sub does not support delaying delivery of individual messages.
   * @throws IllegalStateException If this queue was constructed without a [Publisher].
   */
  override fun send(
      messageBody: String,
      customAttributes: Map<String, String>,
      systemAttributes: Map<String, String>,
      delay: Duration?,
  ): MessageId {
    val publisher =
        this.publisher
            ?: throw IllegalStateException(
                "Cannot send to PubSubQueue without a Publisher. Provide a Publisher when " +
                    "constructing the queue, or use PubSubTopic to publish.",
            )

    val messageId =
        try {
          val pubsubMessage =
              PubsubMessage.newBuilder()
                  .setData(ByteString.copyFromUtf8(messageBody))
                  // Pub/Sub has a single string->string attribute map, with no separate "system"
                  // attributes. We merge both, letting custom attributes win on key collisions.
                  .putAllAttributes(systemAttributes + customAttributes)
                  .build()

          publisher.publish(pubsubMessage).get()
        } catch (e: Exception) {
          observer.onSendException(e, messageBody)
        }

    observer.onSendSuccess(messageId = messageId, messageBody = messageBody)
    return MessageId(messageId)
  }

  override fun poll(): List<Message> {
    val pullRequest =
        PullRequest.newBuilder()
            .setSubscription(subscriptionName)
            .setMaxMessages(MAX_MESSAGES_PER_PULL)
            .build()

    return subscriber.pullCallable().call(pullRequest).receivedMessagesList.map { receivedMessage ->
      pubsubMessageToInternalFormat(receivedMessage, source = subscriptionName)
    }
  }

  /** Acknowledges the message, removing it from the subscription. */
  override fun delete(message: Message) {
    subscriber
        .acknowledgeCallable()
        .call(
            AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionName)
                .addAckIds(message.receiptHandle)
                .build(),
        )
  }

  override fun retry(message: Message) {
    backoffService.increaseVisibilityTimeout(message, subscriptionName)
  }

  internal companion object {
    /** The maximum number of messages to receive in a single pull request. */
    internal const val MAX_MESSAGES_PER_PULL: Int = 10

    /**
     * Key used in [Message.systemAttributes] to hold the Pub/Sub delivery-attempt count (see
     * [ReceivedMessage.getDeliveryAttempt]). Only populated when the subscription has a dead-letter
     * policy.
     */
    internal const val DELIVERY_ATTEMPT_ATTRIBUTE: String = "DeliveryAttempt"

    /**
     * Key used in [Message.systemAttributes] to hold the message's Pub/Sub publish time, in Unix
     * epoch milliseconds.
     */
    internal const val PUBLISH_TIME_ATTRIBUTE: String = "PublishTime"

    /**
     * Key used in [Message.systemAttributes] to hold the message's Pub/Sub ordering key, if set.
     */
    internal const val ORDERING_KEY_ATTRIBUTE: String = "OrderingKey"

    internal val logger = getLogger()
  }
}

internal fun pubsubMessageToInternalFormat(
    receivedMessage: ReceivedMessage,
    source: String,
): Message {
  val pubsubMessage = receivedMessage.message

  val systemAttributes = buildMap {
    val publishTime = pubsubMessage.publishTime
    val publishTimeMillis = publishTime.seconds * 1000 + publishTime.nanos / 1_000_000
    put(PubSubQueue.PUBLISH_TIME_ATTRIBUTE, publishTimeMillis.toString())

    // Pub/Sub only populates the delivery attempt when the subscription has a dead-letter policy;
    // otherwise it's 0, which we omit.
    val deliveryAttempt = receivedMessage.deliveryAttempt
    if (deliveryAttempt > 0) {
      put(PubSubQueue.DELIVERY_ATTEMPT_ATTRIBUTE, deliveryAttempt.toString())
    }

    if (pubsubMessage.orderingKey.isNotEmpty()) {
      put(PubSubQueue.ORDERING_KEY_ATTRIBUTE, pubsubMessage.orderingKey)
    }
  }

  return Message(
      id = MessageId(pubsubMessage.messageId),
      body = pubsubMessage.data.toStringUtf8(),
      // Pub/Sub's "ack ID" plays the same role as SQS's receipt handle: it's the token used to
      // acknowledge the message or change its ack deadline.
      receiptHandle = receivedMessage.ackId,
      systemAttributes = systemAttributes,
      customAttributes = pubsubMessage.attributesMap,
      source = source,
  )
}
