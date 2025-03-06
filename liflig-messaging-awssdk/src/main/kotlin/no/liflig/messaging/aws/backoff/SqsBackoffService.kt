package no.liflig.messaging.aws.backoff

import no.liflig.logging.getLogger
import no.liflig.messaging.Message
import no.liflig.messaging.backoff.BackoffConfig
import no.liflig.messaging.backoff.BackoffService
import no.liflig.messaging.backoff.BackoffService.Companion.getNextVisibilityTimeout
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName

private val log = getLogger {}

/** Calculates and sets exponential backoff for messages on SQS queue */
internal class SqsBackoffService(
    private val sqsClient: SqsClient,
    private val backoffConfig: BackoffConfig
) : BackoffService {
  override fun increaseVisibilityTimeout(message: Message, queueUrl: String) {
    var approximateReceiveCount = 1
    message.systemAttributes[MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString()]
        ?.let { count -> approximateReceiveCount = count.toInt() }
    val receiptHandle = message.receiptHandle
    val nextVisibilityTimeout =
        getNextVisibilityTimeout(
            approximateReceiveCount = approximateReceiveCount,
            maxTimeoutMinutes = backoffConfig.maxTimeOutMinutes,
            backoffFactor = backoffConfig.backoffFactor,
            initialIntervalSeconds = backoffConfig.initialIntervalSeconds,
        )

    log.info {
      field("systemAttributes", message.systemAttributes)
      field("receiveCount", approximateReceiveCount)
      field("nextVisibilityTimeout", nextVisibilityTimeout)
      field("receiptHandle", receiptHandle)
      "Changing message visibility"
    }

    sqsClient.changeMessageVisibility(
        ChangeMessageVisibilityRequest.builder()
            .receiptHandle(receiptHandle)
            .visibilityTimeout(nextVisibilityTimeout)
            .queueUrl(queueUrl)
            .build(),
    )
  }
}
