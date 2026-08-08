package no.liflig.messaging.gcp.backoff

import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import no.liflig.logging.getLogger
import no.liflig.messaging.Message
import no.liflig.messaging.backoff.BackoffConfig
import no.liflig.messaging.backoff.BackoffService
import no.liflig.messaging.backoff.BackoffService.Companion.getNextVisibilityTimeout
import no.liflig.messaging.backoff.BackoffServiceObserver
import no.liflig.messaging.backoff.DefaultBackoffServiceObserver
import no.liflig.messaging.gcp.queue.PubSubQueue

/**
 * Calculates and sets exponential backoff for messages on a Pub/Sub subscription.
 *
 * Pub/Sub does not have an SQS-style "visibility timeout". Instead, a pulled message is leased to
 * the consumer for its "ack deadline"; if the consumer neither acknowledges nor extends the
 * deadline before it passes, the message becomes available for redelivery. We implement backoff by
 * extending the ack deadline ([SubscriberStub.modifyAckDeadlineCallable]) without acknowledging, so
 * the message is redelivered only after the backoff interval has elapsed.
 *
 * Note two caveats compared to SQS:
 * - Pub/Sub caps the ack deadline at [MAX_ACK_DEADLINE_SECONDS] (10 minutes), so longer
 *   [BackoffConfig.maxTimeOutMinutes] values are clamped to that.
 * - The delivery-attempt count used to compute the backoff interval is only populated by Pub/Sub
 *   when the subscription has a
 *   [dead-letter policy](https://cloud.google.com/pubsub/docs/handling-failures#dead_letter_topic)
 *   configured. Without one, every retry uses the initial interval.
 */
internal class PubSubBackoffService(
    private val subscriber: SubscriberStub,
    private val backoffConfig: BackoffConfig,
    private val observer: BackoffServiceObserver = DefaultBackoffServiceObserver(logger),
) : BackoffService {
  /**
   * @param queueUrl The subscription name to extend the ack deadline on (e.g.
   *   `projects/{project}/subscriptions/{subscription}`).
   */
  override fun increaseVisibilityTimeout(message: Message, queueUrl: String) {
    val deliveryAttempt =
        message.systemAttributes[PubSubQueue.DELIVERY_ATTEMPT_ATTRIBUTE]?.toIntOrNull() ?: 1

    val nextVisibilityTimeout =
        getNextVisibilityTimeout(
                approximateReceiveCount = deliveryAttempt,
                maxTimeoutMinutes = backoffConfig.maxTimeOutMinutes,
                backoffFactor = backoffConfig.backoffFactor,
                initialIntervalSeconds = backoffConfig.initialIntervalSeconds,
            )
            // Pub/Sub rejects ack deadlines above 600 seconds.
            .coerceAtMost(MAX_ACK_DEADLINE_SECONDS)

    observer.onIncreaseVisibilityTimeout(
        message,
        nextVisibilityTimeout = nextVisibilityTimeout,
        approximateReceiveCount = deliveryAttempt,
    )

    subscriber
        .modifyAckDeadlineCallable()
        .call(
            ModifyAckDeadlineRequest.newBuilder()
                .setSubscription(queueUrl)
                .addAckIds(message.receiptHandle)
                .setAckDeadlineSeconds(nextVisibilityTimeout)
                .build(),
        )
  }

  internal companion object {
    /** Pub/Sub's maximum allowed ack deadline, in seconds. */
    internal const val MAX_ACK_DEADLINE_SECONDS: Int = 600

    internal val logger = getLogger()
  }
}
