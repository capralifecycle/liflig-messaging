@file:Suppress("unused") // This is a library

package no.liflig.messaging

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds
import no.liflig.logging.getLogger
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
    name: String = "MessagePoller",
    private val observer: MessagePollerObserver =
        DefaultMessagePollerObserver(
            pollerName = name,
            loggingMode = queue.observer?.loggingMode ?: MessageLoggingMode.JSON,
        ),
) : AutoCloseable {
  private val executor: ExecutorService =
      Executors.newFixedThreadPool(concurrentPollers, MessagePollerThreadFactory(namePrefix = name))

  public fun start() {
    observer.onPollerStartup()

    for (i in 0 until concurrentPollers) {
      executor.submit(::runPollLoop)
    }
  }

  private fun runPollLoop() {
    while (!isStopped()) {
      try {
        poll()
      } catch (e: Throwable) {
        if (isStopped(cause = e)) {
          break
        }

        observer.onPollException(e)

        /** See [MessagePoller.POLLER_RETRY_TIMEOUT]. */
        Thread.sleep(POLLER_RETRY_TIMEOUT.inWholeMilliseconds)
      }
    }
  }

  private fun poll() {
    val messages = queue.poll()
    observer.onPoll(messages)

    for (message in messages) {
      val stopped: Boolean =
          observer.wrapMessageProcessing(message) {
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
              return@wrapMessageProcessing false
            } catch (e: Exception) {
              if (isStopped(cause = e)) {
                return@wrapMessageProcessing true
              }

              observer.onMessageException(message, e)
              queue.retry(message)
              return@wrapMessageProcessing false
            }
          }
      if (stopped) {
        return
      }
    }
  }

  /** Stops all poller threads currently running. Does not wait for them to shut down. */
  override fun close() {
    observer.onPollerShutdown()

    executor.shutdown()

    // Give opportunity for poller threads to shut themselves down, before forcing shutdown.
    // We just yield instead of sleeping here, as we don't want to block closing - and we would have
    // to sleep for up to 20 seconds in order to wait for all polling to finish.
    Thread.yield()

    executor.shutdownNow()
  }

  private fun isStopped(cause: Throwable? = null): Boolean {
    val stopped = executor.isShutdown || Thread.currentThread().isInterrupted
    if (stopped) {
      observer.onPollerThreadStopped(cause)
    }
    return stopped
  }

  internal companion object {
    internal val logger = getLogger {}

    /**
     * We sleep for this duration if we encounter an exception in [runPollLoop]. This is to avoid an
     * infinite loop, in the case that `Queue.poll` always throws an exception (which may happen if
     * the queue has been misconfigured). Such an infinite loop would spam logs continuously.
     *
     * It's not a concern to sleep for this duration even if the exception from `Queue.poll` is just
     * a temporary problem (such as a network failure), because `Queue.poll` can already take up to
     * 20 seconds when polling. So delaying further polling by 10 seconds should not be an issue.
     */
    internal val POLLER_RETRY_TIMEOUT = 10.seconds
  }
}

/** Thread factory to set custom names for threads spawned by [MessagePoller.executor]. */
private class MessagePollerThreadFactory(private val namePrefix: String) : ThreadFactory {
  private val threadCount = AtomicInteger(1)

  override fun newThread(runnable: Runnable): Thread {
    return Thread(runnable, "${namePrefix}-${threadCount.getAndIncrement()}")
  }
}
