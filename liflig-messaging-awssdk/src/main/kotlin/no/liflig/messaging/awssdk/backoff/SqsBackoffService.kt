package no.liflig.messaging.awssdk.backoff

import no.liflig.logging.getLogger
import no.liflig.messaging.Message
import no.liflig.messaging.backoff.BackoffConfig
import no.liflig.messaging.backoff.BackoffService
import no.liflig.messaging.backoff.BackoffService.Companion.getNextVisibilityTimeout
import no.liflig.messaging.backoff.BackoffServiceObserver
import no.liflig.messaging.backoff.DefaultBackoffServiceObserver
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName

/** Calculates and sets exponential backoff for messages on an SQS queue. */
internal class SqsBackoffService(
    private val sqsClient: SqsClient,
    private val backoffConfig: BackoffConfig,
    private val observer: BackoffServiceObserver = DefaultBackoffServiceObserver(logger),
) : BackoffService {
  override fun increaseVisibilityTimeout(message: Message, queueUrl: String) {
    val approximateReceiveCount =
        message.systemAttributes[MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString()]
            ?.toInt() ?: 1

    val nextVisibilityTimeout =
        getNextVisibilityTimeout(
            approximateReceiveCount = approximateReceiveCount,
            maxTimeoutMinutes = backoffConfig.maxTimeOutMinutes,
            backoffFactor = backoffConfig.backoffFactor,
            initialIntervalSeconds = backoffConfig.initialIntervalSeconds,
        )

    observer.onIncreaseVisibilityTimeout(
        message,
        nextVisibilityTimeout = nextVisibilityTimeout,
        approximateReceiveCount = approximateReceiveCount,
    )

    sqsClient.changeMessageVisibility(
        ChangeMessageVisibilityRequest.builder()
            .receiptHandle(message.receiptHandle)
            .visibilityTimeout(nextVisibilityTimeout)
            .queueUrl(queueUrl)
            .build(),
    )
  }

  internal companion object {
    internal val logger = getLogger()
  }
}
