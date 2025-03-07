package no.liflig.messaging.awssdk.testutils

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import no.liflig.messaging.DefaultMessagePollerObserver
import no.liflig.messaging.Message

/**
 * [no.liflig.messaging.MessagePollerObserver] implementation to observe message/poll exceptions in
 * tests.
 */
internal class TestMessagePollerObserver : DefaultMessagePollerObserver() {
  private var observedMessageException: Throwable? = null
  private var observedPollException: Throwable? = null
  private val lock = ReentrantLock()

  fun observedMessageException(): Throwable? = lock.withLock { observedMessageException }

  fun observedPollException(): Throwable? = lock.withLock { observedPollException }

  override fun onMessageException(message: Message, exception: Throwable) {
    super.onMessageException(message, exception)

    lock.withLock { observedMessageException = exception }
  }

  override fun onPollException(exception: Throwable) {
    super.onPollException(exception)

    lock.withLock { observedPollException = exception }
  }

  fun reset() {
    lock.withLock {
      observedMessageException = null
      observedPollException = null
    }
  }
}
