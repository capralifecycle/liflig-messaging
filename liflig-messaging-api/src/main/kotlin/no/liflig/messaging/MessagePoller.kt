@file:Suppress("unused") // This is a library

package no.liflig.messaging

import kotlin.concurrent.thread
import kotlin.time.Duration.Companion.seconds
import no.liflig.logging.Logger
import no.liflig.logging.field
import no.liflig.logging.getLogger
import no.liflig.logging.withLoggingContext
import no.liflig.messaging.queue.Queue

/**
 * Polls the given [Queue][no.liflig.messaging.queue.Queue] for messages, and passes them to the
 * given [MessageProcessor]. If processing succeeded, the message is deleted, otherwise we backoff
 * to retry later.
 *
 * @param concurrentPollers Number of threads to spawn. Each thread continuously polls the queue (in
 *   the SQS implementation, message polling waits for up to 20 seconds if there are no available
 *   messages, so continuously polling is not a concern).
 * @param name Used as a prefix for thread names, and if using [DefaultMessagePollerObserver], this
 *   is included in the logs. If you're running multiple MessagePollers in your application, you
 *   should provide a more specific name here, to make debugging easier.
 */
public class MessagePoller(
    private val queue: Queue,
    private val messageProcessor: MessageProcessor,
    private val concurrentPollers: Int = 1,
    private val name: String = "MessagePoller",
    private val observer: MessagePollerObserver = DefaultMessagePollerObserver(pollerName = name),
) {
  public fun start() {
    observer.onPollerStartup()

    for (i in 1..concurrentPollers) {
      thread(name = "${name}-${i}") {
        while (true) {
          try {
            poll()
          } catch (e: Exception) {
            observer.onPollException(e)

            Thread.sleep(POLLER_RETRY_TIMEOUT.inWholeMilliseconds)
          }
        }
      }
    }
  }

  private fun poll() {
    val messages = queue.poll()
    observer.onPoll(messages)

    for (message in messages) {
      // Put message ID and body in logging context, so we can track logs for this message
      withLoggingContext(field("queueMessageId", message.id)) {
        try {
          observer.onMessageProcessing(message)

          when (val result = messageProcessor.process(message)) {
            is ProcessingResult.Success -> {
              observer.onMessageSuccess(message)
              queue.delete(message)
            }
            is ProcessingResult.Failure -> {
              observer.onMessageFailure(message, result)
              if (result.retry) {
                queue.retry(message)
              } else {
                queue.delete(message)
              }
            }
          }
        } catch (e: Exception) {
          observer.onMessageException(message, e)
          queue.retry(message)
        }
      }
    }
  }

  internal companion object {
    internal val logger = getLogger {}

    private val POLLER_RETRY_TIMEOUT = 10.seconds
  }
}

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
    logger.info { "${logPrefix}Processing message from queue" }
  }

  override fun onMessageSuccess(message: Message) {
    logger.info { "${logPrefix}Successfully processed message. Deleting from queue" }
  }

  override fun onMessageFailure(message: Message, result: ProcessingResult.Failure) {
    logger.at(level = result.severity, cause = result.cause) {
      if (result.retry) {
        "${logPrefix}Message processing failed. Will be retried from queue"
      } else {
        "${logPrefix}Message processing failed, with retry disabled. Deleting message from queue"
      }
    }
  }

  override fun onMessageException(message: Message, exception: Exception) {
    logger.error(exception) {
      "${logPrefix}Message processing failed unexpectedly. Will be retried from queue"
    }
  }
}
