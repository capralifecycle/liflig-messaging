package no.liflig.messaging.queue

import java.time.Duration
import no.liflig.logging.ExceptionWithLogFields
import no.liflig.logging.LogField
import no.liflig.logging.getLogger
import no.liflig.messaging.Message

/**
 * A message queue that you can send messages to or poll messages from.
 *
 * This library provides 2 implementations:
 * - `SqsQueue` from the `liflig-messaging-awssdk` module, for AWS SQS (Simple Queue Service)
 * - [MockQueue] for tests
 */
public interface Queue {
  /**
   * @param customAttributes See [Message.customAttributes].
   * @param systemAttributes See [Message.systemAttributes].
   * @param delay Set this to delay sending the message until after the given duration. Maximum
   *   allowed value in SQS is 15 minutes. This argument is ignored in [MockQueue].
   * @throws MessageSendingException If we failed to send the message.
   */
  public fun send(
      messageBody: String,
      customAttributes: Map<String, String> = emptyMap(),
      systemAttributes: Map<String, String> = emptyMap(),
      delay: Duration? = null
  )

  /**
   * Polls the queue for available messages.
   *
   * The implementation in `SqsQueue.poll` (from `liflig-messaging-awssdk`) polls messages for up to
   * 20 seconds, or until it's received 10 messages, whichever comes first.
   */
  public fun poll(): List<Message>

  /**
   * Deletes the message from the queue, either because it was successfully processed, or because
   * processing failed with retry disabled.
   */
  public fun delete(message: Message)

  /**
   * Pushes the message back to the queue, typically because processing failed.
   *
   * The implementation in `SqsQueue.retry` (from `liflig-messaging-awssdk`) increases the message's
   * [visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
   * to do exponential backoff.
   */
  public fun retry(message: Message)

  /**
   * [DefaultMessagePollerObserver][no.liflig.messaging.DefaultMessagePollerObserver] wants to know
   * the [MessageLoggingMode][no.liflig.messaging.MessageLoggingMode] of the poller's queue, to use
   * the same mode for its own logging. This info lies on the queue's observer. We therefore expose
   * an optional observer property here on the `Queue` interface, overriding it in `SqsQueue` to
   * point to its actual observer.
   *
   * If you implement a custom `Queue`, you should consider overriding this property if your
   * implementation uses a `QueueObserver`.
   */
  public val observer: QueueObserver?
    get() = null

  public companion object {
    internal val logger = getLogger()
  }
}

/**
 * Wraps an underlying send exception with extra context for debugging, such as the message body
 * that failed to send.
 *
 * @param message Note that this is the exception message, not the queue message. The queue message
 *   is included in [logFields], logged when the exception is passed to `liflig-logging`.
 * @param logFields See [no.liflig.logging.ExceptionWithLogFields].
 */
public class MessageSendingException
internal constructor(
    override val message: String,
    override val cause: Throwable,
    logFields: List<LogField>,
) : ExceptionWithLogFields(logFields)
