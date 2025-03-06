package no.liflig.messaging.api

import no.liflig.logging.LogLevel

/**
 * Interface for handling messages received from a queue.
 *
 * You implement your custom message processing with this interface. Then, construct a
 * [MessagePoller] using your `MessageProcessor` implementation and a
 * [Queue][no.liflig.messaging.api.queue.Queue]. The message poller will then poll messages from the
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
      /**
       * The severity of the failure.
       *
       * [DefaultMessagePollerObserver.onMessageFailure] uses this as the log level when logging a
       * message processing failure. By default, the ERROR log level is used, but you may want to
       * lower this if it represents an expected failure.
       */
      val severity: LogLevel = LogLevel.ERROR,
  ) : ProcessingResult()
}
