package org.sagebionetworks.bridge.workers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.workers.dynamodb.ReplicaProcessorFactory;
import org.sagebionetworks.bridge.workers.dynamodb.ReplicaStreams;
import org.sagebionetworks.bridge.workers.dynamodb.ReplicaUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.google.common.base.StandardSystemProperty;

@Configuration
public class BridgeWorkersConfig {

    // *** Config *** //

    private static final String CONFIG_FILE = "workers.conf";
    private static final String HOME_CONFIG_FILE = "/.bridge/" + CONFIG_FILE;
    private static final String DYNAMO_SOURCE_ENDPOINT = "dynamodb.us-east-1.amazonaws.com";
    private static final String DYNAMO_REPLICA_ENDPOINT = "dynamodb.us-west-2.amazonaws.com";
    private static final String STREAMS_ADAPTER_ENDPOINT = "streams.dynamodb.us-east-1.amazonaws.com";
    private static final String REPLICA_CLOUDWATCH_ENDPOINT = "monitoring.us-west-2.amazonaws.com";
    private static final String REPLICA_SELECTED_TABLES = "replica.selected.tables";

    @Bean(name = "config")
    public Config config() {
        final ClassLoader classLoader = BridgeWorkersConfig.class.getClassLoader();
        final Path templateConfig = Paths.get(classLoader.getResource(CONFIG_FILE).getPath());
        final Path localConfig = Paths.get(StandardSystemProperty.USER_HOME.value() + HOME_CONFIG_FILE);
        try {
            if (Files.exists(localConfig)) {
                return new PropertiesConfig(templateConfig, localConfig);
            }
            return new PropertiesConfig(templateConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // *** AWS clients *** //

    @Bean(name = "dynamoSource")
    public AmazonDynamoDB dynamoSource() {
        return new AmazonDynamoDBClient().withEndpoint(DYNAMO_SOURCE_ENDPOINT);
    }

    @Bean(name = "dynamoReplica")
    public AmazonDynamoDB dynamoReplica() {
        return new AmazonDynamoDBClient().withEndpoint(DYNAMO_REPLICA_ENDPOINT);
    }

    @Bean(name = "streamsAdapter")
    public AmazonKinesis streamsAdapter() {
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient();
        adapterClient.setEndpoint(STREAMS_ADAPTER_ENDPOINT);
        return adapterClient;
    }

    @Bean(name = "streamsCredentials")
    public AWSCredentialsProvider streamsCredentials() {
        return new DefaultAWSCredentialsProviderChain();
    }

    @Bean(name = "replicaCloudWatch")
    public AmazonCloudWatch replicaCloudWatch() {
        return new AmazonCloudWatchClient().withEndpoint(REPLICA_CLOUDWATCH_ENDPOINT);
    }

    // *** DDB Streams Workers *** //

    @Bean(name = "replicaSourceTables")
    public List<String> replicaSourceTables(final Config config, final AmazonDynamoDB dynamoSource) {
        return Collections.unmodifiableList(Environment.PROD == config.getEnvironment() ?
                ReplicaUtils.getTables(config, dynamoSource) :
                ReplicaUtils.getTables(config.getList(REPLICA_SELECTED_TABLES), config, dynamoSource));
    }

    @Bean(name = "replicaTables")
    public List<String> replicaTables(final List<String> replicaSourceTables,
            final AmazonDynamoDB dynamoSource, final AmazonDynamoDB dynamoReplica) {
        return Collections.unmodifiableList(ReplicaUtils.getReplicaTables(replicaSourceTables, dynamoSource, dynamoReplica));
    }

    @Bean(name = "replicaStreams")
    public ReplicaStreams replicaStreams(final List<String> replicaSourceTables, final AmazonDynamoDB dynamoSource) {
        return new ReplicaStreams(replicaSourceTables, dynamoSource);
    }

    @Bean(name = "replicaWorkers")
    public List<Worker> replicaWorkers(final ReplicaStreams replicaStreams,
            final AWSCredentialsProvider streamsCredentials,
            final AmazonKinesis streamsAdapter, final AmazonDynamoDB dynamoReplica,
            final AmazonCloudWatch replicaCloudWatch) {
        return replicaStreams.getStreams().stream()
                .map(dynamoStream -> {
                    final String fqTableName = dynamoStream.getTableName();
                    final String appName = "replica-worker-" + fqTableName;
                    final String workerId = UUID.randomUUID().toString();
                    final String streamArn = dynamoStream.getStreamArn();
                    final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                            appName, streamArn, streamsCredentials, workerId)
                            .withMaxRecords(1000).withIdleTimeBetweenReadsInMillis(500)
                            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
                    return new Worker(new ReplicaProcessorFactory(fqTableName, dynamoReplica),
                            kinesisConfig, streamsAdapter, dynamoReplica, replicaCloudWatch);
                }).collect(Collectors.toList());
    }

    @Bean(name = "uploadStatusWorker")
    public Worker uploadStatusWorker(final ReplicaStreams replicaStreams) {
        // TODO: To be implemented
        return null;
    }
}
