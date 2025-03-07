package no.liflig.messaging.lambda.testutils

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.kotest.matchers.collections.shouldContainExactly

internal fun createSqsEvent(vararg messages: SQSEvent.SQSMessage): SQSEvent {
  return SQSEvent().apply { records = messages.toList() }
}

internal fun SQSBatchResponse.shouldHaveFailedMessages(vararg messages: SQSEvent.SQSMessage) {
  batchItemFailures.shouldContainExactly(messages.map { BatchItemFailure(it.messageId) })
}
