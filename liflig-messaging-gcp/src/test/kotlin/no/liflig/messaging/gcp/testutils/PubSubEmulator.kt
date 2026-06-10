package no.liflig.messaging.gcp.testutils

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName

internal const val PROJECT_ID = "test-project"

internal fun createPubSubEmulatorContainer(): PubSubEmulatorContainer =
    PubSubEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"),
        )
        .withReuse(true)

/**
 * Helper that connects to a running [PubSubEmulatorContainer] over a plaintext gRPC channel, and
 * creates topics, subscriptions, [Publisher]s and [SubscriberStub]s wired up to talk to it.
 *
 * The emulator requires no credentials, so we use a [NoCredentialsProvider] throughout.
 */
internal class PubSubEmulator(container: PubSubEmulatorContainer) : AutoCloseable {
  private val channel: ManagedChannel =
      ManagedChannelBuilder.forTarget(container.emulatorEndpoint).usePlaintext().build()

  private val channelProvider: TransportChannelProvider =
      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))

  private val credentialsProvider: CredentialsProvider = NoCredentialsProvider.create()

  fun createTopic(topicId: String): TopicName {
    val topicName = TopicName.of(PROJECT_ID, topicId)
    TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build(),
        )
        .use { it.createTopic(topicName) }
    return topicName
  }

  fun createSubscription(
      subscriptionId: String,
      topicName: TopicName,
      ackDeadlineSeconds: Int = 10,
  ): SubscriptionName {
    val subscriptionName = SubscriptionName.of(PROJECT_ID, subscriptionId)
    SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build(),
        )
        .use {
          it.createSubscription(
              subscriptionName.toString(),
              topicName.toString(),
              PushConfig.getDefaultInstance(),
              ackDeadlineSeconds,
          )
        }
    return subscriptionName
  }

  fun createPublisher(topicName: TopicName): Publisher =
      Publisher.newBuilder(topicName)
          .setChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build()

  fun createSubscriberStub(): SubscriberStub =
      GrpcSubscriberStub.create(
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(channelProvider)
              .setCredentialsProvider(credentialsProvider)
              .build(),
      )

  override fun close() {
    channel.shutdownNow()
  }
}
