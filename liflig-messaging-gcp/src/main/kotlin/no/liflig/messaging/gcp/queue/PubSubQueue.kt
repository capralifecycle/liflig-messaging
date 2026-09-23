@file:Suppress("unused") // This is a library

package no.liflig.messaging.gcp.queue

import com.google.api.gax.grpc.GrpcCallContext
import com.google.api.gax.rpc.DeadlineExceededException
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.ByteString
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.ReceivedMessage
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import java.time.Duration
import java.util.concurrent.ExecutionException
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
 *   that sent messages come back around on [poll]. This is not verified.
 *
 * Note that [send] does _not_ behave like sending to an SQS queue: it publishes to the topic, so
 * the message is delivered to _every_ subscription on that topic, not just the one this queue polls
 * from. If other subscriptions exist on the topic, they will receive the message too. Prefer
 * [PubSubTopic][no.liflig.messaging.gcp.topic.PubSubTopic] when publishing to a topic with multiple
 * subscribers, to make the fan-out explicit.
 *
 * ### Trace context propagation
 *
 * Messages polled from the subscription get their [Message.context] populated from the W3C trace
 * context attributes (`googclient_traceparent` / `googclient_tracestate`) that the Pub/Sub client
 * library adds on publish. The client library only adds these if the publisher was built with
 * OpenTelemetry tracing enabled. The OpenTelemetry Java agent does _not_ do this for you, so for
 * trace context to propagate from the publishing service, build its [Publisher] like this:
 * ```
 * Publisher.newBuilder(topicName)
 *     .setEnableOpenTelemetryTracing(true)
 *     .setOpenTelemetry(GlobalOpenTelemetry.get())
 *     .build()
 * ```
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
              // The observer only uses this in logs for sent messages. Those are published to the
              // publisher's topic, not to the subscription, so we log the topic name.
              queueUrl = publisher?.topicNameString ?: subscriptionName,
              logger,
              loggingMode,
          ),
      backoffService = PubSubBackoffService(subscriber, backoffConfig),
  )

  /**
   * Publishes a message to the topic backing this queue's subscription. The message is delivered to
   * every subscription on that topic, not just this queue's (see the class documentation).
   *
   * @param customAttributes Sent as Pub/Sub message attributes. These are received in
   *   [Message.customAttributes] when polled.
   * @param systemAttributes Not supported: Pub/Sub has no system attributes that can be set when
   *   publishing (unlike SQS's `AWSTraceHeader`). Must be empty.
   * @param delay Not supported: Pub/Sub cannot delay delivery of individual messages. Must be null
   *   or zero.
   * @throws UnsupportedOperationException If [systemAttributes] is non-empty, or a non-zero [delay]
   *   is given.
   * @throws IllegalStateException If this queue was constructed without a [Publisher].
   */
  override fun send(
      messageBody: String,
      customAttributes: Map<String, String>,
      systemAttributes: Map<String, String>,
      delay: Duration?,
  ): MessageId {
    if (delay != null && !delay.isZero) {
      throw UnsupportedOperationException(
          "PubSubQueue does not support delayed sending, since Pub/Sub cannot delay delivery of " +
              "individual messages (got delay: ${delay})",
      )
    }

    if (systemAttributes.isNotEmpty()) {
      throw UnsupportedOperationException(
          "PubSubQueue does not support sending system attributes, since Pub/Sub has no system " +
              "attributes that can be set when publishing (got keys: ${systemAttributes.keys}). " +
              "Use customAttributes instead",
      )
    }

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
                  .putAllAttributes(customAttributes)
                  .build()

          publisher.publish(pubsubMessage).get()
        } catch (e: Exception) {
          observer.onSendException(e, messageBody)
        }

    observer.onSendSuccess(messageId = messageId, messageBody = messageBody)
    return MessageId(messageId)
  }

  /**
   * Pulls up to [MAX_MESSAGES_PER_PULL] messages from the subscription. If none are available, the
   * pull waits for up to [POLL_TIMEOUT] (like SQS's 20-second long polling), then returns an empty
   * list.
   *
   * @throws InterruptedException If the polling thread is interrupted while waiting for messages.
   */
  override fun poll(): List<Message> {
    val pullRequest =
        PullRequest.newBuilder()
            .setSubscription(subscriptionName)
            .setMaxMessages(MAX_MESSAGES_PER_PULL)
            .build()

    val future =
        subscriber
            .pullCallable()
            .futureCall(
                pullRequest,
                GrpcCallContext.createDefault().withTimeoutDuration(POLL_TIMEOUT),
            )

    val response =
        try {
          // We use futureCall + get instead of pullCallable().call(), since call() waits
          // uninterruptibly. This way, MessagePoller.close() can stop a poller that's waiting for
          // messages.
          future.get()
        } catch (e: InterruptedException) {
          future.cancel(true)
          Thread.currentThread().interrupt()
          throw e
        } catch (e: ExecutionException) {
          when (val cause = e.cause) {
            // If no messages arrive before the pull's deadline, Pub/Sub may respond with
            // DEADLINE_EXCEEDED instead of an empty response. That just means there were no
            // messages.
            is DeadlineExceededException -> return emptyList()
            null -> throw e
            else -> throw cause
          }
        }

    return response.receivedMessagesList.map { receivedMessage ->
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
     * How long a pull waits for messages before returning empty. Matches the 20 seconds used by
     * `SqsQueue` for long polling, which [MessagePoller][no.liflig.messaging.MessagePoller] is
     * designed around. Without this, the Pub/Sub client's default deadline of 60 seconds applies.
     */
    internal val POLL_TIMEOUT: Duration = Duration.ofSeconds(20)

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

    /**
     * Prefix used by Pub/Sub client libraries for attributes carrying internal tracing metadata
     * (see [PubsubMessage.extractContext]). These are stripped from [Message.customAttributes], the
     * same way SQS's system attributes never end up there.
     */
    internal const val GOOGCLIENT_ATTRIBUTE_PREFIX: String = "googclient_"

    /** Attribute holding the W3C `traceparent` value, when publish-side tracing is enabled. */
    internal const val TRACEPARENT_ATTRIBUTE: String = "googclient_traceparent"

    /** Attribute holding the W3C `tracestate` value, when publish-side tracing is enabled. */
    internal const val TRACESTATE_ATTRIBUTE: String = "googclient_tracestate"

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
      customAttributes =
          pubsubMessage.attributesMap.filterKeys {
            !it.startsWith(PubSubQueue.GOOGCLIENT_ATTRIBUTE_PREFIX)
          },
      source = source,
      context = pubsubMessage.extractContext(),
  )
}

/**
 * Attempts to pull W3C trace context from the `googclient_traceparent`/`googclient_tracestate`
 * attributes that Pub/Sub client libraries set on publish when tracing is enabled (see
 * https://cloud.google.com/pubsub/docs/open-telemetry-tracing).
 *
 * Unlike SQS's equivalent (`SQSMessage.extractContext` in `liflig-messaging-awssdk`), this does not
 * use the propagator registered in the global OpenTelemetry instance. The Pub/Sub client library
 * always injects these attributes with [W3CTraceContextPropagator], regardless of the configured
 * propagators, so we extract with the same propagator. Otherwise, extraction would silently fail
 * when the global propagator is not W3C (e.g. X-Ray only).
 *
 * Returns null if no trace context attribute is present, leaving it up to the caller to decide
 * whether to start a new trace.
 */
internal fun PubsubMessage.extractContext(): Context? {
  val traceparent = this.attributesMap[PubSubQueue.TRACEPARENT_ATTRIBUTE] ?: return null

  val carrier = buildMap {
    put("traceparent", traceparent)
    attributesMap[PubSubQueue.TRACESTATE_ATTRIBUTE]?.let { put("tracestate", it) }
  }

  val getter =
      object : TextMapGetter<Map<String, String>> {
        override fun keys(carrier: Map<String, String>): Iterable<String> = carrier.keys

        override fun get(carrier: Map<String, String>?, key: String): String? = carrier?.get(key)
      }

  return W3CTraceContextPropagator.getInstance().extract(Context.root(), carrier, getter)
}
