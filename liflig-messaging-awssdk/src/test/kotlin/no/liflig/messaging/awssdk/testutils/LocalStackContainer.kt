package no.liflig.messaging.awssdk.testutils

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sqs.SqsClient

internal fun LocalStackContainer.createSqsClient(): SqsClient =
    SqsClient.builder()
        .endpointOverride(endpoint)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
        )
        .region(Region.of(region))
        .build()

internal fun LocalStackContainer.createSnsClient(): SnsClient =
    SnsClient.builder()
        .endpointOverride(getEndpointOverride(LocalStackContainer.Service.SNS))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
        )
        .region(Region.of(region))
        .build()

internal fun createLocalstackContainer(
    services: Set<LocalStackContainer.EnabledService> =
        setOf(
            LocalStackContainer.Service.SNS,
            LocalStackContainer.Service.SQS,
        )
): LocalStackContainer {

  return LocalStackContainer(DockerImageName.parse("localstack/localstack:4.2.0"))
      .withReuse(true)
      .withServices(*services.toTypedArray())
}
