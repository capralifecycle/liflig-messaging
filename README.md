# liflig-messaging

Library for applications that process messages asynchronously. Provides interfaces and
implementations for message queues, topics, pollers and processors.

The library is split into modules:

- `liflig-messaging-core` provides the `Queue`, `Topic` and `MessageProcessor` interfaces, as well
  as the `MessagePoller` class for polling messages from a queue.
- `liflig-messaging-awssdk` implements the `Queue` interface for AWS SQS and the `Topic` interface
  for AWS SNS, using the AWS SDK.
- `liflig-messaging-sqs-lambda` provides a function for processing messages in AWS Lambda functions
  that use SQS as the event source. It improves failure handling for individual messages in a batch,
  and allows you to use the same `MessageProcessor` interface as in long-running services.

**Contents:**

- [Usage](#usage)
  - [Processing messages from a queue](#processing-messages-from-a-queue)
    - [Long-running services](#long-running-services)
    - [AWS Lambda functions](#aws-lambda-functions)
  - [Sending messages to a queue](#sending-messages-to-a-queue)
  - [Publishing to a message topic](#publishing-to-a-message-topic)
- [Adding to your project](#adding-to-your-project)
- [Maintainer's guide](#maintainers-guide)

## Usage

### Processing messages from a queue

The main thing that your application has to concern itself with, is implementing the
`MessageProcessor` interface. This is where your application-specific message processing logic goes.

Example:

```kotlin
import no.liflig.messaging.Message
import no.liflig.messaging.MessageProcessor
import no.liflig.messaging.ProcessingResult

class ExampleEventProcessor : MessageProcessor {
  override fun process(message: Message): ProcessingResult {
    val event = ExampleEvent.fromJson(message.body)

    handleEvent(event)

    return ProcessingResult.Success
  }
}
```

How you use your `MessageProcessor` depends on if your application is a long-running service, or an
AWS Lambda function.

#### Long-running services

For a long-running service, you'll use `MessagePoller`. You pass your `MessageProcessor` and a
`Queue` implementation to its constructor, and call `messagePoller.start()` on application start-up.
This spawns a thread that runs side-by-side with your service, polling messages from the queue and
passing them to your processor.

If you use `liflig-messaging-awssdk`, you can use the `SqsQueue` implementation for AWS SQS (Simple
Queue Service).

```kotlin
import no.liflig.messaging.MessagePoller
import no.liflig.messaging.Queue
import no.liflig.messaging.awssdk.SqsQueue
import software.amazon.awssdk.services.sqs.SqsClient

class App(config: Config) {
  val inputQueue: Queue = SqsQueue(SqsClient.create(), config.inputQueueUrl)

  val messagePoller = MessagePoller(
    queue = inputQueue,
    messageProcessor = ExampleEventProcessor(),
  )

  fun start() {
    messagePoller.start()
  }
}

fun main() {
  App(Config.load()).start()
}
```

#### AWS Lambda functions

For an AWS Lambda function, you'll construct your `MessageProcessor` implementation in your
`LambdaHandler`, and then call `handleLambdaSqsEvent` (from `liflig-messaging-sqs-lambda`) in your
handler method:

```kotlin
import no.liflig.messaging.lambda.handleLambdaSqsEvent

class LambdaHandler(
  private val eventProcessor: MessageProcessor = ExampleEventProcessor(),
) {
  fun handle(sqsEvent: SQSEvent): SQSBatchResponse {
    return handleLambdaSqsEvent(sqsEvent, messageProcessor)
  }
}
```

> [!IMPORTANT]
>
> In order for `handleLambdaSqsEvent` to work correctly, your handler method must return
> `SQSBatchResponse`. And in order for that to work, you have to configure batch item failures on
> your Lambda <-> SQS integration. In AWS CDK, you do this on the `SqsEventSource`:
>
> ```ts
> myLambda.addEventSource(
>   new SqsEventSource(myQueue, { reportBatchItemFailures: true })
> );
> ```
>
> For more on the reason behind this, see the docstring on `handleLambdaSqsEvent`.

### Sending messages to a queue

Construct a `Queue` like you would for the `MessagePoller` example above. You can then call `send`
on it to send a message.

```kotlin
import no.liflig.messaging.Queue
import no.liflig.messaging.awssdk.SqsQueue
import software.amazon.awssdk.services.sqs.SqsClient

class ExampleEventSender(
  private val outputQueue: Queue = SqsQueue(SqsClient.create(), queueUrl = "..."),
) {
  fun sendEvent(event: ExampleEvent) {
    outputQueue.send(event.toJson())
  }
}
```

### Publishing to a message topic

The `Topic` interface from `liflig-messaging-core` represents a pub-sub message topic.
`liflig-messaging-awssdk` provides `SnsTopic`, an implementation of this interface for AWS SNS
(Simple Notification Service).

```kotlin
import no.liflig.messaging.Topic
import no.liflig.messaging.awssdk.SnsTopic
import software.amazon.awssdk.services.sns.SnsClient

class ExampleEventPublisher(
  private val eventTopic: Topic = SnsTopic(SnsClient.create(), topicArn = "..."),
) {
  fun publishEvent(event: ExampleEvent) {
    eventTopic.publish(event.toJson())
  }
}
```

## Adding to your project

We use Maven as the example build system here.

First, add the `core` module:

<!-- @formatter:off -->
```xml
<dependency>
  <groupId>no.liflig</groupId>
  <artifactId>liflig-messaging-core</artifactId>
  <version>${liflig-messaging.version}</version>
</dependency>
```
<!-- @formatter:on -->

Then, add extra modules depending on your use-case:

- If your application is a long-running service, and you want to use the AWS SDK implementations:
  <!-- @formatter:off -->
  ```xml
  <dependency>
    <groupId>no.liflig</groupId>
    <artifactId>liflig-messaging-awssdk</artifactId>
    <version>${liflig-messaging.version}</version>
  </dependency>
  ```
  <!-- @formatter:on -->
- If your application is an AWS Lambda function with an SQS event source:
  <!-- @formatter:off -->
  ```xml
  <dependency>
    <groupId>no.liflig</groupId>
    <artifactId>liflig-messaging-sqs-lambda</artifactId>
    <version>${liflig-messaging.version}</version>
  </dependency>
  ```
  <!-- @formatter:on -->

## Maintainer's guide

### Build & Test

```sh
mvn clean install
```

### Lint code

```sh
mvn spotless:check
```

### Format code

```sh
mvn spotless:apply
```
