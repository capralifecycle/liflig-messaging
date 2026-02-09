package no.liflig.messaging.queue

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import java.util.UUID
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import no.liflig.messaging.DefaultMessagePollerObserver
import no.liflig.messaging.Message
import no.liflig.messaging.MessageId
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.MessagePollerObserver
import no.liflig.messaging.ProcessingResult
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

internal class MockQueueTest {
  val queue = MockQueue()

  @BeforeEach
  fun reset() {
    queue.clear()
  }

  @Test
  fun `messages that fail processing with retry disabled are added to failedMessages`() {
    val message = createMessage("test")
    queue.sentMessages.add(message)

    val messagePoller =
        MessagePoller(
            queue,
            messageProcessor = { ProcessingResult.Failure(retry = false) },
        )
    messagePoller.poll()

    queue.failedMessages.shouldContainExactly(message)
    queue.sentMessages.shouldBeEmpty()
    queue.processedMessages.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test poll`() {
    /**
     * We want to test that [MockQueue.poll] works as expected: It should block the [MessagePoller]
     * until there are messages to receive on the queue. So we expect the poller to only poll from
     * the queue once when we just send a single message to the queue.
     */
    var pollCount = 0
    var polledMessages: List<Message>? = null

    /**
     * Use [CountDownLatch] along with a custom [MessagePollerObserver] so that we can wait for
     * certain points in the MessagePoller's lifecycle.
     */
    val pollerStartedSignal = CountDownLatch(1)
    val pollCompleteSignal = CountDownLatch(1)

    val messagePoller =
        MessagePoller(
            queue,
            messageProcessor = { ProcessingResult.Success },
            observer =
                object : DefaultMessagePollerObserver() {
                  override fun onPollerStartup() {
                    super.onPollerStartup()

                    pollerStartedSignal.countDown()
                  }

                  override fun onPoll(messages: List<Message>) {
                    super.onPoll(messages)

                    pollCount++
                    polledMessages = messages
                    pollCompleteSignal.countDown()
                  }
                },
        )
    messagePoller.use {
      messagePoller.start()

      /**
       * Let the poller start before we send a message, so we can verify [MockQueue.poll] does not
       * return before there's a message on the queue.
       */
      pollerStartedSignal.await()

      queue.send("test")

      /** Wait for the message to be polled. */
      pollCompleteSignal.await()

      pollCount.shouldBe(1)
      polledMessages.shouldNotBeNull().map { it.body }.shouldBe(listOf("test"))
    }
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitProcessed`() {
    val messages = listOf(createMessage("1"), createMessage("2"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitProcessed(messages.size) }

    val producer = thread {
      messages.forEach {
        queue.delete(it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
    queue.processedMessages.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitSent`() {
    val messageBodies = listOf("1", "2")

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitSent(messageBodies.size) }

    val producer = thread {
      messageBodies.forEach {
        queue.send(messageBody = it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().map { it.body }.shouldBe(messageBodies)
    queue.sentMessages.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitFailed`() {
    val messages = listOf(createMessage("1"), createMessage("2"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitFailed(messages.size) }

    val producer = thread {
      /**
       * Add failed messages both with and without retry, to verify that both these are cleared
       * after the call to `awaitFailed`.
       */
      queue.retry(messages[0])
      Thread.yield() // Yield to allow consumer thread to interleave
      queue.deleteFailed(messages[1])
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
    queue.failedMessages.shouldBeEmpty()
    queue.failedMessagesWithRetry.shouldBeEmpty()
    queue.failedMessagesWithoutRetry.shouldBeEmpty()
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitFailedWithRetry`() {
    /**
     * Add some previously failed messages without retry, to verify that these are not removed from
     * the [MockQueue.failedMessages] list.
     */
    val failedMessagesWithoutRetry = listOf(createMessage("1"), createMessage(("2")))
    queue.sentMessages.addAll(failedMessagesWithoutRetry)
    failedMessagesWithoutRetry.forEach { queue.deleteFailed(it) }
    queue.failedMessages.shouldBe(failedMessagesWithoutRetry)
    queue.failedMessagesWithoutRetry.shouldBe(failedMessagesWithoutRetry)

    /** Now fail some messages with retry enabled. */
    val messages = listOf(createMessage("3"), createMessage("4"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitFailedWithRetry(messages.size) }

    val producer = thread {
      messages.forEach {
        queue.retry(it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
    queue.failedMessagesWithRetry.shouldBeEmpty()

    /** Verify that previously failed messages without retry still remain on the queue. */
    queue.failedMessages.shouldBe(failedMessagesWithoutRetry)
    queue.failedMessagesWithoutRetry.shouldBe(failedMessagesWithoutRetry)
  }

  @RepeatedTest(10) // Repeat test for more variations of threads interleaving
  fun `test awaitFailedWithoutRetry`() {
    /**
     * Add some previously failed messages with retry, to verify that these are not removed from the
     * [MockQueue.failedMessages] list.
     */
    val failedMessagesWithRetry = listOf(createMessage("1"), createMessage(("2")))
    queue.sentMessages.addAll(failedMessagesWithRetry)
    failedMessagesWithRetry.forEach { queue.retry(it) }
    queue.failedMessages.shouldBe(failedMessagesWithRetry)
    queue.failedMessagesWithRetry.shouldBe(failedMessagesWithRetry)

    /** Now fail some messages with retry disabled. */
    val messages = listOf(createMessage("3"), createMessage("4"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitFailedWithoutRetry(messages.size) }

    val producer = thread {
      messages.forEach {
        queue.deleteFailed(it)
        Thread.yield()
      }
    }

    consumer.join()
    producer.join()

    result.shouldNotBeNull().shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
    queue.failedMessagesWithoutRetry.shouldBeEmpty()

    /** Verify that previously failed messages with retry still remain on the queue. */
    queue.failedMessages.shouldBe(failedMessagesWithRetry)
    queue.failedMessagesWithRetry.shouldBe(failedMessagesWithRetry)
  }

  @Test
  fun `test successful expectProcessed`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.processedMessages.addAll(messages)

    val actualMessages = queue.expectProcessed(2)
    actualMessages.shouldBe(messages)
    queue.processedMessages.shouldBeEmpty()
  }

  @Test
  fun `test successful expectSent`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.sentMessages.addAll(messages)

    val actualMessages = queue.expectSent(2)
    actualMessages.shouldBe(messages)
    queue.sentMessages.shouldBeEmpty()
  }

  @Test
  fun `test successful expectFailed`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.failedMessages.addAll(messages)
    queue.failedMessagesWithRetry.add(messages[0])
    queue.failedMessagesWithoutRetry.add(messages[1])

    val actualMessages = queue.expectFailed(2)
    actualMessages.shouldBe(messages)
    queue.failedMessages.shouldBeEmpty()
    queue.failedMessagesWithRetry.shouldBeEmpty()
    queue.failedMessagesWithoutRetry.shouldBeEmpty()
  }

  @Test
  fun `test successful expectFailedWithRetry`() {
    val messagesWithRetry = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.failedMessagesWithRetry.addAll(messagesWithRetry)

    /** Also test messages without retry, to verify that they remain on the queue after. */
    val messagesWithoutRetry = listOf(createMessage("msg3"), createMessage("msg4"))
    queue.failedMessagesWithoutRetry.addAll(messagesWithoutRetry)
    queue.failedMessages.addAll(messagesWithRetry + messagesWithoutRetry)

    val actualMessages = queue.expectFailedWithRetry(2)
    actualMessages.shouldBe(messagesWithRetry)
    queue.failedMessagesWithRetry.shouldBeEmpty()

    queue.failedMessages.shouldBe(messagesWithoutRetry)
    queue.failedMessagesWithoutRetry.shouldBe(messagesWithoutRetry)
  }

  @Test
  fun `test successful expectFailedWithoutRetry`() {
    val messagesWithoutRetry = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.failedMessagesWithoutRetry.addAll(messagesWithoutRetry)

    /** Also test messages with retry, to verify that they remain on the queue after. */
    val messagesWithRetry = listOf(createMessage("msg3"), createMessage("msg4"))
    queue.failedMessagesWithRetry.addAll(messagesWithRetry)
    queue.failedMessages.addAll(messagesWithoutRetry + messagesWithRetry)

    val actualMessages = queue.expectFailedWithoutRetry(2)
    actualMessages.shouldBe(messagesWithoutRetry)
    queue.failedMessagesWithoutRetry.shouldBeEmpty()

    queue.failedMessages.shouldBe(messagesWithRetry)
    queue.failedMessagesWithRetry.shouldBe(messagesWithRetry)
  }

  @Test
  fun `test expectProcessed exception`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.processedMessages.addAll(messages)

    val exception = shouldThrow<IllegalStateException> { queue.expectProcessed(3) }
    exception.message.shouldBe(
        """
        Expected 3 processed messages on queue, got 2:
        	1: msg1
        	2: msg2
        """
            .trimIndent()
    )
  }

  @Test
  fun `test expectSent exception`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.sentMessages.addAll(messages)

    val exception = shouldThrow<IllegalStateException> { queue.expectSent(3) }
    exception.message.shouldBe(
        """
        Expected 3 sent messages on queue, got 2:
        	1: msg1
        	2: msg2
        """
            .trimIndent()
    )
  }

  @Test
  fun `test expectFailed exception`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.failedMessages.addAll(messages)

    val exception = shouldThrow<IllegalStateException> { queue.expectFailed(3) }
    exception.message.shouldBe(
        """
        Expected 3 failed messages on queue, got 2:
        	1: msg1
        	2: msg2
        """
            .trimIndent()
    )
  }

  @Test
  fun `test expectFailedWithRetry exception`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.failedMessagesWithRetry.addAll(messages)

    val exception = shouldThrow<IllegalStateException> { queue.expectFailedWithRetry(3) }
    exception.message.shouldBe(
        """
        Expected 3 failed messages (with retry enabled) on queue, got 2:
        	1: msg1
        	2: msg2
        """
            .trimIndent()
    )
  }

  @Test
  fun `test expectFailedWithoutRetry exception`() {
    val messages = listOf(createMessage("msg1"), createMessage("msg2"))
    queue.failedMessagesWithoutRetry.addAll(messages)

    val exception = shouldThrow<IllegalStateException> { queue.expectFailedWithoutRetry(3) }
    exception.message.shouldBe(
        """
        Expected 3 failed messages (with retry disabled) on queue, got 2:
        	1: msg1
        	2: msg2
        """
            .trimIndent()
    )
  }
}

private fun createMessage(body: String) =
    Message(
        id = MessageId(UUID.randomUUID().toString()),
        body = body,
        systemAttributes = emptyMap(),
        customAttributes = emptyMap(),
    )
