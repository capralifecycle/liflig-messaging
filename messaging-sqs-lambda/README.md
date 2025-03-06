# Messaging SQS Lambda
## Usage

Add maven dependency
```xml
<dependency>
  <groupId>no.liflig</groupId>
  <artifactId>messaging-sqs-lambda</artifactId>
  <version>x.x.x</version>
</dependency>
```

Implement a `MessageProcessor`

```kotlin
  class ExampleProcessor() : MessageProcessor {
    override fun process(message: Message): ProcessingResult {
      return if (messageIsGood(message)) {
        ProcessingResult.Success
      } else {
        ProcessingResult.Failure(retry = false)
      }
    }
  }
```
Invoke `handleLambdaSqsEvent` in your lambda handler
```kotlin
class LambdaHandler(
    private val messageProcessor: MessageProcessor = ExampleProcessor()
) {
  fun handle(sqsEvent: SQSEvent): SQSBatchResponse {
    return handleLambdaSqsEvent(sqsEvent, messageProcessor)
  }
}

```
