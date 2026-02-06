package no.liflig.messaging.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import kotlin.concurrent.withLock

/**
 * Acquires the given lock, and then calls [tryGetResult]. If it returns a non-null result, then the
 * lock is released and the result is returned. Otherwise, we wait on the given condition variable
 * (which releases the lock and waits for some other thread to call [Condition.signal], upon which
 * this thread will reacquire the lock). We keep checking [tryGetResult] and waiting on the given
 * condition variable until we get a non-null result, or until the given timeout expires (in which
 * case a [TimeoutException] is thrown).
 *
 * Assumes that the condition variable has been created by calling [Lock.newCondition] on the given
 * lock.
 */
internal fun <ResultT : Any> await(
    lock: Lock,
    condition: Condition,
    timeout: Duration?,
    tryGetResult: () -> ResultT?,
): ResultT {
  lock.withLock {
    while (true) {
      val result = tryGetResult()
      if (result != null) {
        return result
      }

      if (timeout == null) {
        condition.await()
      } else {
        val success = condition.await(timeout.toMillis(), TimeUnit.MILLISECONDS)
        if (!success) {
          throw TimeoutException("Timed out waiting for messages")
        }
      }
    }
  }
}

internal class TimeoutException(message: String) : RuntimeException(message)
