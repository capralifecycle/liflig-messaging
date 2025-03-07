package no.liflig.messaging

import no.liflig.logging.Logger
import no.liflig.logging.getLogger

/**
 * Interface for observing various events in [MessagePoller]'s polling loop.
 *
 * A default implementation is provided by [DefaultMessagePollerObserver], which uses
 * `liflig-logging` to log descriptive messages for each of these events.
 */
public interface MessagePollerObserver {
  /** Called when [MessagePoller] starts up. */
  public fun onPollerStartup()

  /** Called when [MessagePoller] polls messages from its queue. */
  public fun onPoll(messages: List<Message>)

  /** Called when an exception is thrown when [MessagePoller] polls from its queue. */
  public fun onPollException(exception: Exception)

  /**
   * Called when [MessagePoller] starts processing a message, before passing it to the
   * [MessageProcessor].
   */
  public fun onMessageProcessing(message: Message)

  /** Called when a [MessageProcessor] returns [ProcessingResult.Success]. */
  public fun onMessageSuccess(message: Message)

  /** Called when a [MessageProcessor] returns [ProcessingResult.Failure]. */
  public fun onMessageFailure(message: Message, result: ProcessingResult.Failure)

  /** Called when a [MessageProcessor] throws an exception while processing a message. */
  public fun onMessageException(message: Message, exception: Exception)
}

/**
 * Default implementation of [MessagePollerObserver], using `liflig-logging` to log descriptive
 * messages for the various events in [MessagePoller]'s polling loop.
 */
public open class DefaultMessagePollerObserver(
    /**
     * Will be added as a prefix to all logs, to distinguish between different `MessagePoller`s. If
     * passing `null` here, no prefix will be added.
     */
    pollerName: String? = "MessagePoller",
    /**
     * Defaults to [MessagePoller]'s logger, so the logger name will show as:
     * `no.liflig.messaging.MessagePoller`.
     *
     * If you want a different logger name, you can construct your own logger (using [getLogger])
     * and pass it here.
     */
    protected val logger: Logger = MessagePoller.logger,
    protected val loggingMode: MessageLoggingMode = MessageLoggingMode.JSON,
) : MessagePollerObserver {
  private val logPrefix = if (pollerName != null) "[${pollerName}] " else ""

  override fun onPollerStartup() {
    logger.info { "${logPrefix}Starting polling" }
  }

  override fun onPoll(messages: List<Message>) {
    if (messages.isNotEmpty()) {
      logger.info { "${logPrefix}Received ${messages.size} messages from queue" }
    }
  }

  override fun onPollException(exception: Exception) {
    logger.error(exception) { "${logPrefix}Failed to poll messages. Retrying" }
  }

  override fun onMessageProcessing(message: Message) {
    logger.info {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      "${logPrefix}Processing message from queue"
    }
  }

  override fun onMessageSuccess(message: Message) {
    logger.info {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      "${logPrefix}Successfully processed message. Deleting from queue"
    }
  }

  override fun onMessageFailure(message: Message, result: ProcessingResult.Failure) {
    logger.at(level = result.severity, cause = result.cause) {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      if (result.retry) {
        "${logPrefix}Message processing failed. Will be retried from queue"
      } else {
        "${logPrefix}Message processing failed, with retry disabled. Deleting message from queue"
      }
    }
  }

  override fun onMessageException(message: Message, exception: Exception) {
    logger.error(exception) {
      addMessageBodyToLog("queueMessage", message.body, loggingMode)
      "${logPrefix}Message processing failed unexpectedly. Will be retried from queue"
    }
  }
}
