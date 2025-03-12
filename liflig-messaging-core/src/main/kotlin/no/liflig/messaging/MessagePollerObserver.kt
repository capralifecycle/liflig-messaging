package no.liflig.messaging

import no.liflig.logging.Logger
import no.liflig.logging.field
import no.liflig.logging.withLoggingContext

/**
 * Interface for observing various events in [MessagePoller]'s polling loop.
 *
 * A default implementation is provided by [DefaultMessagePollerObserver], which uses
 * `liflig-logging` to log descriptive messages for these events.
 */
public interface MessagePollerObserver {
  /** Called when [MessagePoller] starts up. */
  public fun onPollerStartup()

  /** Called when [MessagePoller] polls messages from its queue. */
  public fun onPoll(messages: List<Message>)

  /** Called when an exception is thrown when [MessagePoller] polls from its queue. */
  public fun onPollException(exception: Throwable)

  /** Called by [MessagePoller.close]. */
  public fun onPollerShutdown()

  /**
   * Called when a [MessagePoller] thread detects that it has been interrupted. This is typically
   * due to [MessagePoller.close] having been called.
   *
   * @param cause If the thread detected interruption in the context of an exception, it is passed
   *   here.
   */
  public fun onPollerThreadStopped(cause: Throwable?)

  /**
   * Called when [MessagePoller] starts processing a message, before passing it to the
   * [MessageProcessor].
   *
   * This is called inside the scope of [wrapMessageProcessing].
   */
  public fun onMessageProcessing(message: Message)

  /**
   * Called when [MessageProcessor] returns [ProcessingResult.Success].
   *
   * This is called inside the scope of [wrapMessageProcessing].
   */
  public fun onMessageSuccess(message: Message)

  /**
   * Called when [MessageProcessor] returns [ProcessingResult.Failure].
   *
   * This is called inside the scope of [wrapMessageProcessing].
   */
  public fun onMessageFailure(message: Message, result: ProcessingResult.Failure)

  /**
   * Called when [MessageProcessor] throws an exception while processing a message.
   *
   * This is called inside the scope of [wrapMessageProcessing].
   */
  public fun onMessageException(message: Message, exception: Throwable)

  /**
   * Wraps [MessagePoller]'s code for processing the given message (including the call to
   * [MessageProcessor.process]). This allows you to add scope-based context to the message
   * processing. For example, [DefaultMessagePollerObserver] uses
   * [no.liflig.logging.withLoggingContext] to add the queue message ID to the logging context.
   *
   * The implementation of this method MUST call the given [messageProcessingBlock] once, and only
   * once.
   *
   * @return The same type as the given [messageProcessingBlock] (so you must return the result of
   *   calling the block).
   */
  public fun <ReturnT> wrapMessageProcessing(
      message: Message,
      messageProcessingBlock: () -> ReturnT
  ): ReturnT

  /**
   * Wraps [MessagePoller]'s main polling loop (including message processing), as well as startup
   * and shutdown. This allows you to add scope-based context to the whole lifetime of a message
   * poller (the given [pollerBlock] will not exit until the poller is closed). For example,
   * [DefaultMessagePollerObserver] uses [no.liflig.logging.withLoggingContext] to add the message
   * poller name to the logging context, so users can distinguish between logs from different
   * pollers.
   *
   * The implementation of this method MUST call the given [pollerBlock] once, and only once.
   *
   * All other methods on this interface are called in the scope of [pollerBlock].
   *
   * @return The same type as the given [pollerBlock] (so you must return the result of calling the
   *   lambda).
   */
  public fun <ReturnT> wrapPoller(pollerBlock: () -> ReturnT): ReturnT
}

/**
 * Default implementation of [MessagePollerObserver], using `liflig-logging` to log descriptive
 * messages for the various events in [MessagePoller]'s polling loop.
 *
 * @param pollerName Will be added as a field to all logs in the context of the message poller, so
 *   you can distinguish between logs from different pollers. The log field key is
 *   `"messagePollerName"`. If you set this to `null`, no log field will be added.
 * @param logger Defaults to [MessagePoller]'s logger, so the logger name will show as:
 *   `no.liflig.messaging.MessagePoller`. If you want a different logger name, you can construct
 *   your own logger (using [no.liflig.logging.getLogger]) and pass it here.
 * @param loggingMode Controls how message bodies are logged. Defaults to [MessageLoggingMode.JSON],
 *   which tries to include the message as raw JSON, but checks that it's valid JSON first.
 */
public open class DefaultMessagePollerObserver(
    protected val pollerName: String? = "MessagePoller",
    protected val logger: Logger = MessagePoller.logger,
    protected val loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
) : MessagePollerObserver {
  override fun onPollerStartup() {
    logger.info { "Starting message poller" }
  }

  override fun onPoll(messages: List<Message>) {
    if (messages.isNotEmpty()) {
      logger.info {
        "Received ${messages.size} ${if (messages.size == 1) "message" else "messages"} from queue"
      }
    }
  }

  override fun onPollException(exception: Throwable) {
    logger.error(exception) {
      "Failed to poll messages. Retrying in ${MessagePoller.POLLER_RETRY_TIMEOUT.inWholeSeconds} seconds"
    }
  }

  override fun onPollerShutdown() {
    logger.info { "Shutting down message poller" }
  }

  override fun onPollerThreadStopped(cause: Throwable?) {
    logger.info(cause) { "Message poller thread stopped" }
  }

  override fun onMessageProcessing(message: Message) {
    logger.info {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      "Processing message from queue"
    }
  }

  override fun onMessageSuccess(message: Message) {
    logger.info {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      "Successfully processed message. Deleting from queue"
    }
  }

  override fun onMessageFailure(message: Message, result: ProcessingResult.Failure) {
    logger.at(level = result.severity, cause = result.cause) {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      if (result.retry) {
        "Message processing failed. Will be retried from queue"
      } else {
        "Message processing failed, with retry disabled. Deleting message from queue"
      }
    }
  }

  override fun onMessageException(message: Message, exception: Throwable) {
    logger.error(exception) {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      "Message processing failed unexpectedly. Will be retried from queue"
    }
  }

  override fun <ReturnT> wrapMessageProcessing(
      message: Message,
      messageProcessingBlock: () -> ReturnT
  ): ReturnT {
    // Puts message ID in logging context, so we can trace logs for the message
    return withLoggingContext(field("queueMessageId", message.id), block = messageProcessingBlock)
  }

  override fun <ReturnT> wrapPoller(pollerBlock: () -> ReturnT): ReturnT {
    if (pollerName == null) {
      return pollerBlock()
    }

    return withLoggingContext(field("messagePollerName", pollerName), block = pollerBlock)
  }
}
