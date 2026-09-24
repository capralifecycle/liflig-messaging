package no.liflig.messaging.gcp.utils

import com.google.api.core.ApiFuture
import java.util.concurrent.ExecutionException

/**
 * Waits for the future to complete, and returns its result. Unlike calling [ApiFuture.get]
 * directly, this:
 * - Throws the exception that the future failed with, instead of wrapping it in
 *   [ExecutionException], so callers and logs see the actual Pub/Sub error.
 * - Restores the thread's interrupt flag when interrupted. [ApiFuture.get] clears the flag when it
 *   throws [InterruptedException], so without this, code further up (such as
 *   [MessagePoller][no.liflig.messaging.MessagePoller]) could not see that the thread should stop.
 *
 * @throws InterruptedException If the thread is interrupted while waiting.
 */
internal fun <T> ApiFuture<T>.getUnwrapped(): T {
  try {
    return get()
  } catch (e: ExecutionException) {
    throw e.cause ?: e
  } catch (e: InterruptedException) {
    Thread.currentThread().interrupt()
    throw e
  }
}
