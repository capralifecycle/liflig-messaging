@file:Suppress("unused") // This is a library

package no.liflig.messaging

import kotlin.concurrent.thread
import kotlin.time.Duration.Companion.seconds
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
    private val observer: MessagePollerObserver =
        DefaultMessagePollerObserver(
            pollerName = name,
            loggingMode = queue.observer?.loggingMode ?: MessageLoggingMode.JSON,
        ),
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
