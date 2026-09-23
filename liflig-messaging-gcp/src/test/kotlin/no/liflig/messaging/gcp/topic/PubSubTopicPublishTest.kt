package no.liflig.messaging.gcp.topic

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutures
import com.google.api.core.SettableApiFuture
import com.google.api.gax.grpc.GrpcStatusCode
import com.google.api.gax.rpc.NotFoundException
import com.google.cloud.pubsub.v1.Publisher
import io.grpc.Status
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.every
import io.mockk.mockk
import no.liflig.messaging.MessageId
import no.liflig.messaging.topic.MessagePublishingException
import org.junit.jupiter.api.Test

internal class PubSubTopicPublishTest {
  @Test
  fun `publish returns the published message ID`() {
    val topic = createTopic(ApiFutures.immediateFuture("published-id"))

    topic.publish("test-message") shouldBe MessageId("published-id")
  }

  @Test
  fun `publish exception has the publish error as cause, not ExecutionException`() {
    val publishError = NotFoundException(null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false)
    val topic = createTopic(ApiFutures.immediateFailedFuture(publishError))

    val exception = shouldThrow<MessagePublishingException> { topic.publish("test-message") }

    exception.cause shouldBeSameInstanceAs publishError
  }

  @Test
  fun `publish restores interrupt flag when interrupted`() {
    // Never completes, like a publish waiting for a response
    val topic = createTopic(SettableApiFuture.create())

    Thread.currentThread().interrupt()
    try {
      val exception = shouldThrow<MessagePublishingException> { topic.publish("test-message") }

      exception.cause.shouldBeInstanceOf<InterruptedException>()
      Thread.currentThread().isInterrupted shouldBe true
    } finally {
      // Clear the interrupt flag, so it doesn't leak into other tests
      Thread.interrupted()
    }
  }

  private fun createTopic(publishResult: ApiFuture<String>): PubSubTopic {
    val publisher = mockk<Publisher>()
    every { publisher.topicNameString } returns "projects/test-project/topics/test-topic"
    every { publisher.publish(any()) } returns publishResult

    return PubSubTopic(publisher)
  }
}
