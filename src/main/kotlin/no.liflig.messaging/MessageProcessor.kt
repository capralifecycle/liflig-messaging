package no.liflig.messaging

/**
 * Interface for handling messages received from a queue.
 *
 * You implement your custom message processing with this interface. Then, construct a
 * [MessagePoller] using your `MessageProcessor` implementation and a
 * [Queue][no.liflig.messaging.queue.Queue]. The message poller will then poll messages from the
 * queue service and call your message processor with each message.
 */
public interface MessageProcessor {
  public fun process(message: Message): ProcessingResult
}

public sealed class ProcessingResult {
  public data object Success : ProcessingResult()

  public data class Failure(
      /** Whether the failed message should be retried. */
      val retry: Boolean,
      /**
       * If the message failed due to an exception, you can attach it here to include it in the
       * logs.
       */
      val cause: Throwable? = null,
  ) : ProcessingResult()
}
