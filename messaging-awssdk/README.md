# Messaging AWS SDK
## Usage

Add maven dependency
```xml
<dependency>
  <groupId>no.liflig</groupId>
  <artifactId>messaging-awssdk</artifactId>
  <version>x.x.x</version>
</dependency>
```

### Poll from an SQS queue

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
Send processor into `MessagePoller` and invoke `start`
```kotlin
  val queueUrl = "example.com"
  val processor = ExampleProcessor()
  
  val queue = SqsQueue(
    SqsClient.create(),
    queueUrl,
    messagesAreValidJson = true,
  )
 
  val poller = MessagePoller(
    queueService = queue,
    messageProcessor = processor,
    name = "ExamplePoller",
  )
  
  poller.start()
```

### Send messages to an SQS queue
Use an `SqsQueue`
```kotlin
    val queueUrl = "example.com"
    
    val queue = SqsQueue(
      SqsClient.create(),
      queueUrl,
      messagesAreValidJson = false,
    )

queue.send(someStringBody)
```

### Send messages to an SNS topic
Use an `SnsTopic`
```kotlin
    val topicArn = "example:arn"
    
    val topic = SnsTopic(
        SnsClient.create(), 
        topicArn, 
        messagesAreValidJson = true
    )

    topic.publish(someStringBody)
```
