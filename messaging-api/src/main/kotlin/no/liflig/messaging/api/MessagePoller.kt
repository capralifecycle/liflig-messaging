@file:Suppress("unused") // This is a library

package no.liflig.messaging.api

import kotlin.collections.isNotEmpty
import kotlin.concurrent.thread
import no.liflig.logging.field
import no.liflig.logging.getLogger
import no.liflig.logging.rawJsonField
import no.liflig.logging.withLoggingContext
import no.liflig.messaging.api.queue.Queue

private val log = getLogger {}
private const val POLLER_RETRY_TIMEOUT_SECONDS = 10L

/**
 * Polls the given [Queue][no.liflig.messaging.queue.Queue] for messages, and passes them to the
 * given [MessageProcessor]. If processing succeeded, the message is deleted, otherwise we backoff
 * to retry later.
 *
 * Runs a number of threads equal to [concurrentPollers], which each continuously polls the queue
 * (in the SQS implementation, message polling waits for up to 20 seconds if there are no available
 * messages, so continuously polling is not a concern).
 *
 * [name] is used in logging and as the prefix for thread names. You should provide a more specific
 * name if running multiple MessagePollers, to make debugging easier.
 */
public class MessagePoller(
  private val queue: Queue,
  private val messageProcessor: MessageProcessor,
  private val concurrentPollers: Int = 1,
  private val name: String = "MessagePoller"
) {
  public fun start() {
    log.info { "Starting ${name}" }

    for (i in 1..concurrentPollers) {
      thread(name = "${name}-${i}") {
        while (true) {
          try {
            poll()

          } catch (e: Exception) {
            log.error(e) { "[${name}] Failed to poll messages. Retrying in $POLLER_RETRY_TIMEOUT_SECONDS seconds." }
            Thread.sleep(POLLER_RETRY_TIMEOUT_SECONDS * 1000)
          }
        }
      }
    }
  }

  private fun poll() {
    val messages = queue.poll()

    if (messages.isNotEmpty()) {
      log.info { "[${name}] Received ${messages.size} messages from queue" }
    }

    for (message in messages) {
      // Put message ID and body in logging context, so we can track logs for this message
      withLoggingContext(
          field("queueMessageId", message.id),
          rawJsonField("queueMessage", message.body, validJson = queue.messagesAreValidJson),
      ) {
        try {
          log.info { "[${name}] Processing message from queue" }

          when (val result = messageProcessor.process(message)) {
            is ProcessingResult.Success -> {
              log.info { "[${name}] Successfully processed message. Deleting from queue" }
              queue.delete(message)
            }

            is ProcessingResult.Failure -> {
              if (result.retry) {
                log.warn(result.cause) {
                  "[${name}] Message processing failed. Will be retried from queue"
                }
                queue.retry(message)
              } else {
                log.warn(result.cause) {
                  "[${name}] Message processing failed, with retry disabled. Deleting message from queue"
                }
                queue.delete(message)
              }
            }
          }
        } catch (e: Exception) {
          log.error(e) {
            "[${name}] Message processing failed unexpectedly. Will be retried from queue"
          }
          queue.retry(message)
        }
      }
    }
  }
}
