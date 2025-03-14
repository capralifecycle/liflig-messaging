package no.liflig.messaging

import java.time.Instant

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
     * An identifier of the queue the message was polled from. The type of queue identifier used for
     * this field varies depending on how the message was polled:
     * - `SqsQueue` (in `liflig-messaging-awssdk`) uses the queue URL for this field, since that's
     *   what the AWS SDK takes when polling messages
     * - `handleLambdaSqsEvent` (in `liflig-messaging-sqs-lambda`) uses the queue ARN (Amazon
     *   Resource Name) for this field, since that's what Lambda functions receive in their SQS
     *   event (through the `eventSourceArn` field)
     */
    val source: String? = null,
) {
  /**
   * In AWS SQS, the message's [systemAttributes] contain a `SentTimestamp` with the time that the
   * message was sent to the queue (in Unix epoch milliseconds). This can be useful if you want to
   * know when an event was sent by a client, not just when your server polled it from the queue.
   *
   * See
   * [AWS docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#SQS-ReceiveMessage-request-MessageSystemAttributeNames).
   *
   * @throws IllegalStateException If `SentTimestamp` could not be found in the message's
   *   [systemAttributes]. This may happen if the `ReceiveMessage` request sent to SQS did not
   *   specify that `SentTimestamp` should be returned. However, this should always be included when
   *   using `liflig-messaging`, because `SqsQueue` in `liflig-messaging-awssdk` requests `"All"`
   *   system attributes in its `poll` method, and SQS <-> Lambda integrations always get
   *   `SentTimestamp`.
   * @throws java.lang.NumberFormatException If the format of the `SentTimestamp` attribute value
   *   could not be parsed as a [Long].
   */
  public fun getSqsSentTimestamp(): Instant {
    val sentTimestamp =
        systemAttributes["SentTimestamp"]
            ?: throw IllegalStateException(
                "Expected to find 'SentTimestamp' in SQS message system attributes",
            )

    return Instant.ofEpochMilli(sentTimestamp.toLong())
  }

  /**
   * Adds a `SentTimestamp` system attribute with the given time, using the same epoch millisecond
   * format as SQS. This is useful in tests, for message processors that use [getSqsSentTimestamp].
   *
   * Note that this method returns a _copy_ of the message with the attribute set, since [Message]
   * is immutable.
   */
  public fun setSqsSentTimestamp(time: Instant): Message {
    return this.copy(
        systemAttributes =
            this.systemAttributes + ("SentTimestamp" to time.toEpochMilli().toString()),
    )
  }
}
