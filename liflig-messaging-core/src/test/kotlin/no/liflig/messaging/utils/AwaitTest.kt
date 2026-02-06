package no.liflig.messaging.utils

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import java.time.Duration
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import no.liflig.messaging.queue.MockQueue
import no.liflig.messaging.topic.MockTopic
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

/** Tests the [await] function, which we use in [MockQueue] and [MockTopic]. */
internal class AwaitTest {
  val lock: Lock = ReentrantLock()
  val cond: Condition = lock.newCondition()

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `consumer thread awaits producer thread`() {
    val list = mutableListOf<Int>()
    val targetSize = 3

    var result: List<Int>? = null

    val consumer = thread {
      result =
          await(lock, cond, timeout = null) {
            if (list.size == targetSize) {
              ArrayList(list) // Copy list, for thread safety
            } else {
              null
            }
          }
    }

    val producer = thread {
      repeat(targetSize) { i ->
        lock.withLock {
          list.add(i)
          cond.signalAll()
        }
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldContainExactly(0, 1, 2)
  }

  @Test
  fun `throws TimeoutException on timeout`() {
    shouldThrow<TimeoutException> { await(lock, cond, timeout = Duration.ofMillis(1)) { null } }
  }
}
