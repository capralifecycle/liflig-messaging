# liflig-messaging

Provides a queue service, a message poller and a message processor interface for applications that process events
asynchronously.

## Usage
Se respective readme files

- [Messaging-awssdk](messaging-awssdk/README.md)
- [Messaging-sqs-lambda](messaging-sqs-lambda/README.md)

## Build & Test

```sh
mvn clean install
```

## Lint code

```sh
mvn spottless:check
```

## Format code

```sh
mvn spottless:apply
```
