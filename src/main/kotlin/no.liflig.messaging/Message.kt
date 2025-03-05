package no.liflig.messaging

public data class Message(
    val id: String,
    val body: String,
    /**
     * In SQS, these are a predefined set of valid attribute keys:
     * - `SenderId`
     * - `SentTimestamp`
     * - `ApproximateReceiveCount`
     * - `ApproximateFirstReceiveTimestamp`
     * - `SequenceNumber`
     * - `MessageDeduplicationId`
     * - `MessageGroupId`
     * - `AWSTraceHeader`
     * - `DeadLetterQueueSourceArn`
     *
     * Only `AWSTraceHeader` is currently valid to set when sending a message.
     *
     * See
     * [AWS docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html).
     */
    val systemAttributes: Map<String, String>,
    /**
     * In SQS, these are message attributes with custom keys and values set by us. For example, we
     * use this to forward headers as message attributes in API Gateway endpoints integrated with
     * SQS.
     *
     * See
     * [AWS docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html).
     */
    val customAttributes: Map<String, String>,
    /**
     * When we poll a message from SQS, we get a "receipt handle" that we can use to change the
     * message's visibility timeout (which we use for exponential backoff).
     */
    val receiptHandle: String? = null,

    /**
     * When processing sqs messages from lambda event
     * [com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage] we have access to the
     * eventSource arn
     */
    val eventSource: String? = null
)
