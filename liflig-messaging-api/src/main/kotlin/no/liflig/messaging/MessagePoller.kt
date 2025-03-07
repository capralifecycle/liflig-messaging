@file:Suppress("unused") // This is a library

package no.liflig.messaging

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
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
) : AutoCloseable {
  /** Guards [status] and [threads]. */
  private val lock = ReentrantLock()
  private var status = PollerStatus.STOPPED
  private val threads: MutableList<Thread> = ArrayList(concurrentPollers)
  /** +1 party for the thread that calls [close]. */
  private val latch =
      CyclicBarrier(concurrentPollers + 1) { lock.withLock { status = PollerStatus.STOPPED } }

  public fun start() {
    observer.onPollerStartup()

    lock.withLock {
      if (status != PollerStatus.STOPPED) {
        logger.error { "Tried to start ${name} twice" }
        return
      }

      threads.addAll(
          (1..concurrentPollers).asSequence().map { number ->
            thread(name = "${name}-${number}", block = ::runPollLoop)
          },
      )
      status = PollerStatus.STARTED
    }
  }

  /** Stops all poller threads currently running. */
  override fun close() {
    try {
      observer.onPollerShutdown()
    } catch (e: Exception) {
      // We don't want to fail to shut down the MessagePoller just because the observer failed, so
      // we just log here.
      logger.error(e) {
        "[${name}] Exception thrown by MessagePollerObserver.onPollerShutdown while closing down poller"
      }
    }

    lock.withLock {
      if (status == PollerStatus.STOPPED) {
        return
      }
      status = PollerStatus.STOPPING

      // Give threads 3 seconds to react to PollerStatus.STOPPING, before we start interrupting them
      try {
        Thread.sleep(3_000)
      } catch (_: InterruptedException) {}

      for (thread in threads) {
        thread.interrupt()
      }
      threads.clear()
    }

    // Wait for all threads to exit
    latch.await()
  }

  private fun runPollLoop() {
    try {
      while (!isStopped()) {
        try {
          poll()
        } catch (e: Throwable) {
          if (isStopped(cause = e)) {
            break
          }

          observer.onPollException(e)
          Thread.sleep(POLLER_RETRY_TIMEOUT.inWholeMilliseconds)
        }
      }
    } finally {
      Thread.interrupted() // Clear interrupt status before calling latch.await()
      latch.await()
    }
  }

  private fun poll() {
    val messages = queue.poll()
    observer.onPoll(messages)

    for (message in messages) {
      // Put message ID in logging context, so we can track logs for this message
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
          if (isStopped(cause = e)) {
            return
          }

          observer.onMessageException(message, e)
          queue.retry(message)
        }
      }
    }
  }

  private fun isStopped(cause: Throwable? = null): Boolean {
    val status = lock.withLock { this.status }
    val stopped =
        status == PollerStatus.STOPPING ||
            status == PollerStatus.STOPPED ||
            Thread.currentThread().isInterrupted
    if (stopped) {
      observer.onPollerThreadStopped(cause)
    }
    return stopped
  }

  internal companion object {
    internal val logger = getLogger {}

    internal val POLLER_RETRY_TIMEOUT = 10.seconds
  }
}

private enum class PollerStatus {
  STARTED,
  STOPPING,
  STOPPED,
}
