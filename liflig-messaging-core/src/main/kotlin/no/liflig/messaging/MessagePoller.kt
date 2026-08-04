@file:Suppress("unused") // This is a library

package no.liflig.messaging

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Predicate
import kotlin.concurrent.withLock
import no.liflig.logging.getLogger
import no.liflig.messaging.observability.OpenTelemetryMessagePollerObserver
import no.liflig.messaging.queue.Queue

/**
 * Polls the given [Queue][Queue] for messages, and passes them to the given [MessageProcessor]. If
 * processing succeeded, the message is deleted, otherwise we backoff to retry later.
 *
 * Call [start] on application start-up. This will spawn threads that run side-by-side with your
 * application, continuously polling messages.
 *
 * @param concurrentPollers Number of threads to spawn. Each thread continuously polls the queue (in
 *   the SQS implementation, message polling waits for up to 20 seconds if there are no available
 *   messages, so continuously polling is not a concern).
 * @param name Used as a prefix for thread names, and if using [DefaultMessagePollerObserver], this
 *   is included in the logs. If you're running multiple MessagePollers in your application, you
 *   should provide a more specific name here, to make debugging easier.
 * @param observer See [MessagePollerObserver].
 * @param stopPredicate If you want custom logic to determine when the `MessagePoller`'s polling
 *   loop should stop, you can pass this "stop predicate" function. When an exception is thrown in
 *   the polling loop, this predicate is called with the exception. If it returns true, the poller
 *   stops.
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
    private val stopPredicate: Predicate<Throwable>? = null,
    private val sleep: (Long) -> Unit = Thread::sleep,
) : AutoCloseable {
  private val executor: ExecutorService =
      Executors.newFixedThreadPool(concurrentPollers, MessagePollerThreadFactory(namePrefix = name))

  private val lock = ReentrantLock()
  private var delayNextPollUntil: Instant? = null

  /**
   * Spawns a number of threads equal to [concurrentPollers] (default 1). Each thread continuously
   * polls messages from the queue, passing them to the message processor. The polling continues
   * until [close] is called, or the application exits.
   */
  public fun start() {
    observer.wrapPoller {
      observer.onPollerStartup()

      for (i in 0 until concurrentPollers) {
        executor.submit(::runPollLoop)
      }
    }
  }

  private fun runPollLoop() {
    observer.wrapPoller {
      while (!isStopped()) {
        val delayNextPoll: Duration =
            try {
              poll()
            } catch (e: Throwable) {
              if (isStopped(cause = e)) {
                break
              }

              observer.onPollException(e)

              /** See [POLLER_RETRY_TIMEOUT]. */
              POLLER_RETRY_TIMEOUT
            }

        updateBackoff(delayNextPoll)?.let { sleep(it.toMillis()) }
      }
    }
  }

  /**
   * Takes the wanted backoff from a [poll] invocation, and returns the actual duration the poller
   * thread should wait. All poller threads will wait at least that long on next invocation.
   *
   * This should ensure that concurrent pollers cooperate and agree on how long to pause polling. If
   * poller A wants to wait 10 seconds, and poller B returns 1 second later and wants to continue
   * immediately, then both pollers will respect poller A's decision.
   */
  private fun updateBackoff(backoff: Duration): Duration? {
    val now = Instant.now()

    lock.withLock {
      now.plus(backoff).let {
        if (delayNextPollUntil == null || it > delayNextPollUntil) {
          delayNextPollUntil = it
        }
      }

      return if (now >= delayNextPollUntil) {
        delayNextPollUntil = null
        null
      } else {
        Duration.between(now, delayNextPollUntil)
      }
    }
  }

  internal fun poll(): Duration {
    val messages = queue.poll()
    observer.onPoll(messages)
    var nextDelay = Duration.ZERO

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

                  if (nextDelay < result.backoff) {
                    nextDelay = result.backoff
                  }

                  if (result.retry) {
                    queue.retry(message)
                  } else {
                    queue.deleteFailed(message)
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
        break
      }
    }
    return nextDelay
  }

  /** Stops all poller threads currently running. Does not wait for them to shut down. */
  override fun close() {
    observer.wrapPoller {
      observer.onPollerShutdown()

      executor.shutdown()

      // Give opportunity for poller threads to shut themselves down, before forcing shutdown.
      // We just yield instead of sleeping here, as we don't want to block closing - and we would
      // have to sleep for up to 20 seconds in order to wait for all polling to finish.
      Thread.yield()

      executor.shutdownNow()
    }
  }

  private fun isStopped(cause: Throwable? = null): Boolean {
    var stopped = executor.isShutdown || Thread.currentThread().isInterrupted

    if (stopPredicate != null && cause != null && !stopped) {
      stopped = stopPredicate.test(cause)
    }

    if (stopped) {
      observer.onPollerThreadStopped(cause)
    }
    return stopped
  }

  internal companion object {
    internal val logger = getLogger()

    /**
     * We sleep for this duration if we encounter an exception in [runPollLoop]. This is to avoid an
     * infinite loop, in the case that `Queue.poll` always throws an exception (which may happen if
     * the queue has been misconfigured). Such an infinite loop would spam logs continuously.
     *
     * It's not a concern to sleep for this duration even if the exception from `Queue.poll` is just
     * a temporary problem (such as a network failure), because `Queue.poll` can already take up to
     * 20 seconds when polling. So delaying further polling by 10 seconds should not be an issue.
     */
    internal val POLLER_RETRY_TIMEOUT = Duration.ofSeconds(10)
  }
}

/** Thread factory to set custom names for threads spawned by [MessagePoller.executor]. */
private class MessagePollerThreadFactory(private val namePrefix: String) : ThreadFactory {
  private val threadCount = AtomicInteger(1)

  override fun newThread(runnable: Runnable): Thread {
    return Thread(runnable, "${namePrefix}-${threadCount.getAndIncrement()}")
  }
}

public class MessagePollerBuilder {
  /**
   * Used as a prefix for thread names, and if using logging, this is included in the logs. If
   * you're running multiple MessagePollers in your application, you should provide a more specific
   * name here, to make debugging easier.
   */
  public var pollerName: String = "MessagePoller"

  public var queueName: String = ""

  /**
   * Number of threads to spawn. Each thread continuously polls the queue (in the SQS
   * implementation, message polling waits for up to 20 seconds if there are no available messages,
   * so continuously polling is not a concern).
   */
  public var concurrentPollers: Int = 1

  /**
   * Enables logging via `liflig-logging`. [quiet] controls whether we only log on failure.
   *
   * See [DefaultMessagePollerObserver]
   */
  public var logging: Boolean = true

  /** If true, logging will only occur on failure. If false log each poll and success as well. */
  public var quiet: Boolean = false

  /**
   * Enables OpenTelemetry trace context propagation. A span will be created and set as current for
   * each invocation of [MessageProcessor.process].
   */
  public var tracing: Boolean = false

  /**
   * If you want custom logic to determine when the `MessagePoller`'s polling loop should stop, you
   * can pass this "stop predicate" function. When an exception is thrown in the polling loop, this
   * predicate is called with the exception. If it returns true, the poller stops.
   */
  public var stopPredicate: Predicate<Throwable>? = null

  internal val observers: MutableList<MessagePollerObserver> = mutableListOf()

  public fun observer(observer: MessagePollerObserver) {
    observers.add(observer)
  }
}

/**
 * Build a new [MessagePoller]. Can be configured further in [builder].
 *
 * @sample sample
 */
public fun messagePoller(
    queue: Queue,
    messageProcessor: MessageProcessor,
    builder: MessagePollerBuilder.() -> Unit,
): MessagePoller {
  val builder = MessagePollerBuilder()
  builder.builder()

  if (builder.logging) {
    val logger =
        if (builder.quiet) {
          QuietMessagePollerObserver(
              pollerName = builder.pollerName,
          )
        } else {
          DefaultMessagePollerObserver(
              builder.pollerName,
              loggingMode = queue.observer?.loggingMode ?: MessageLoggingMode.JSON,
          )
        }

    builder.observers.addFirst(logger)
  }

  if (builder.tracing) {
    val tracer =
        OpenTelemetryMessagePollerObserver(
            builder.queueName,
            builder.pollerName,
        )
    builder.observers.addFirst(tracer)
  }

  return MessagePoller(
      queue = queue,
      messageProcessor = messageProcessor,
      concurrentPollers = builder.concurrentPollers,
      name = builder.pollerName,
      observer = ChainedMessagePollerObserver(builder.observers),
      stopPredicate = builder.stopPredicate,
  )
}

private fun sample(queue: Queue, messageProcessor: MessageProcessor): MessagePoller =
    messagePoller(queue, messageProcessor) {
      pollerName = "NoopPoller"
      queueName = "mock-queue"
      tracing = true
    }
