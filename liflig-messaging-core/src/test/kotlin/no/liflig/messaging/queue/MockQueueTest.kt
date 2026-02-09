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
  fun `test awaitFailed`() {
    val messages = listOf(createMessage("1"), createMessage("2"))
    queue.sentMessages.addAll(messages)

    var result: List<Message>? = null
    val consumer = thread { result = queue.awaitFailed(messages.size) }

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
    queue.failedMessages.shouldBeEmpty()
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

    val actualMessages = queue.expectFailed(2)
    actualMessages.shouldBe(messages)
    queue.failedMessages.shouldBeEmpty()
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
}

private fun createMessage(body: String) =
    Message(
        id = MessageId(UUID.randomUUID().toString()),
        body = body,
        systemAttributes = emptyMap(),
        customAttributes = emptyMap(),
    )
