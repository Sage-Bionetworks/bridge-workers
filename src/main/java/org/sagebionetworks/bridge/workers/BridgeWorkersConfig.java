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
import org.sagebionetworks.bridge.dynamodb.DynamoUtils;
import org.sagebionetworks.bridge.redis.JedisOps;
import org.sagebionetworks.bridge.workers.dynamodb.streams.DynamoStream;
import org.sagebionetworks.bridge.workers.dynamodb.streams.DynamoStreams;
import org.sagebionetworks.bridge.workers.dynamodb.streams.ReplicaProcessorFactory;
import org.sagebionetworks.bridge.workers.dynamodb.streams.StreamsUtils;
import org.sagebionetworks.bridge.workers.dynamodb.streams.UploadStatusProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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

@ComponentScan("org.sagebionetworks.bridge.workers")
@EnableScheduling
@Configuration
public class BridgeWorkersConfig {

    private final Logger log = LoggerFactory.getLogger(BridgeWorkersConfig.class);

    // *** Config *** //

    private static final String CONFIG_FILE = "workers.conf";
    private static final String HOME_CONFIG_FILE = "/.bridge/" + CONFIG_FILE;
    private static final String STREAMS_DYNAMO_ENDPOINT = "dynamodb.us-east-1.amazonaws.com";
    private static final String STREAMS_ADAPTER_ENDPOINT = "streams.dynamodb.us-east-1.amazonaws.com";
    private static final String STREAMS_CONFIG_SELECTED_TABLES = "streams.selected.tables";
    private static final String STREAMS_CONFIG_UPLOAD_TABLE = "streams.upload.table";
    private static final String WORKERS_DYNAMO_ENDPOINT = "dynamodb.us-west-2.amazonaws.com";
    private static final String WORKERS_CLOUDWATCH_ENDPOINT = "monitoring.us-west-2.amazonaws.com";

    @Bean
    public Config config() {
        final ClassLoader classLoader = BridgeWorkersConfig.class.getClassLoader();
        final Path templateConfig = Paths.get(classLoader.getResource(CONFIG_FILE).getPath());
        final Path localConfig = Paths.get(StandardSystemProperty.USER_HOME.value() + HOME_CONFIG_FILE);
        try {
            if (Files.exists(localConfig)) {
                log.info("Loading local config " + localConfig + ".");
                return new PropertiesConfig(templateConfig, localConfig);
            }
            log.info("Local config missing and skipped.");
            return new PropertiesConfig(templateConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // *** AWS clients *** //

    @Bean
    public AmazonDynamoDB streamsDynamo() {
        return new AmazonDynamoDBClient().withEndpoint(STREAMS_DYNAMO_ENDPOINT);
    }

    @Bean
    public AmazonKinesis streamsAdapter() {
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient();
        adapterClient.setEndpoint(STREAMS_ADAPTER_ENDPOINT);
        return adapterClient;
    }

    @Bean
    public AWSCredentialsProvider streamsCredentials() {
        return new DefaultAWSCredentialsProviderChain();
    }

    @Bean
    public AmazonDynamoDB workersDynamo() {
        return new AmazonDynamoDBClient().withEndpoint(WORKERS_DYNAMO_ENDPOINT);
    }

    @Bean
    public AmazonCloudWatch workersCloudWatch() {
        return new AmazonCloudWatchClient().withEndpoint(WORKERS_CLOUDWATCH_ENDPOINT);
    }

    // *** Redis *** //

    @Bean
    public JedisPool jedisPool() {

        final Config config = config();

        // Configure pool
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getInt("redis.max.total"));

        // Create pool
        final String host = config.get("redis.host");
        final int port = config.getInt("redis.port");
        final int timeout = config.getInt("redis.timeout");
        final String password = config.get("redis.password");
        final JedisPool jedisPool = Environment.LOCAL == config.getEnvironment() ?
                new JedisPool(poolConfig, host, port, timeout) :
                new JedisPool(poolConfig, host, port, timeout, password);

        // Test pool
        try (Jedis jedis = jedisPool.getResource()) {
            final String result = jedis.ping();
            if (result == null || !"PONG".equalsIgnoreCase(result.trim())) {
                throw new RuntimeException("Redis missing.");
            }
        }

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                jedisPool.destroy();
            }
        }));

        return jedisPool;
    }

    @Bean
    public JedisOps jedisOps() {
        return new JedisOps(jedisPool());
    }

    // *** DDB Streams Workers *** //

    @Bean
    public List<String> streamsTables() {
        final Config config = config();
        final AmazonDynamoDB streamsDynamo = streamsDynamo();
        return Collections.unmodifiableList(Environment.PROD == config.getEnvironment() ?
                StreamsUtils.getTables(config, streamsDynamo) :
                StreamsUtils.getTables(config.getList(STREAMS_CONFIG_SELECTED_TABLES), config, streamsDynamo));
    }

    @Bean
    public List<String> replicaTables() {
        final List<String> streamsTables = streamsTables();
        final AmazonDynamoDB streamsDynamo = streamsDynamo();
        final AmazonDynamoDB workersDynamo = workersDynamo();
        return Collections.unmodifiableList(StreamsUtils.getReplicaTables(streamsTables, streamsDynamo, workersDynamo));
    }

    @Bean
    public DynamoStreams streams() {
        final List<String> streamsTables = streamsTables();
        final AmazonDynamoDB streamsDynamo = streamsDynamo();
        return new DynamoStreams(streamsTables, streamsDynamo);
    }

    @Bean
    public List<Worker> replicaWorkers() {
        final DynamoStreams streams = streams();
        final AWSCredentialsProvider streamsCredentials = streamsCredentials();
        final AmazonKinesis streamsAdapter = streamsAdapter();
        final AmazonDynamoDB workersDynamo = workersDynamo();
        final AmazonCloudWatch workersCloudWatch = workersCloudWatch();
        return streams.getStreams().stream().map(dynamoStream -> {
                    final String fqTableName = dynamoStream.getTableName();
                    final String appName = "replica-worker-" + fqTableName;
                    final String workerId = UUID.randomUUID().toString();
                    final String streamArn = dynamoStream.getStreamArn();
                    final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                            appName, streamArn, streamsCredentials, workerId)
                            .withMaxRecords(1000).withIdleTimeBetweenReadsInMillis(500)
                            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
                    log.info("Creating worker app " + appName + " id " + workerId + ".");
                    return new Worker(new ReplicaProcessorFactory(fqTableName, workersDynamo),
                            kinesisConfig, streamsAdapter, workersDynamo, workersCloudWatch);
                }).collect(Collectors.toList());
    }

    @Bean
    public String uploadStatusRedisKey() {
        final Config config = config();
        final String fqTableName = DynamoUtils.getFullyQualifiedTableName(config.get(STREAMS_CONFIG_UPLOAD_TABLE), config);
        return fqTableName + "-RequestedOrUnknown";
    }

    @Bean
    public Worker uploadStatusWorker() {
        final Config config = config();
        final DynamoStreams streams = streams();
        final AWSCredentialsProvider streamsCredentials = streamsCredentials();
        final AmazonKinesis streamsAdapter = streamsAdapter();
        final AmazonDynamoDB workersDynamo = workersDynamo();
        final AmazonCloudWatch workersCloudWatch = workersCloudWatch();
        final JedisOps jedisOps = jedisOps();
        final String redisKey = uploadStatusRedisKey();
        final String fqTableName = DynamoUtils.getFullyQualifiedTableName(config.get(STREAMS_CONFIG_UPLOAD_TABLE), config);
        final String appName = "upload-worker-" + fqTableName;
        final String workerId = UUID.randomUUID().toString();
        final DynamoStream stream = streams.getStream(fqTableName);
        final String streamArn = stream.getStreamArn();
        final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                appName, streamArn, streamsCredentials, workerId)
                .withMaxRecords(1000).withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        log.info("Creating worker app " + appName + " id " + workerId + ".");
        return new Worker(new UploadStatusProcessorFactory(redisKey, jedisOps),
                kinesisConfig, streamsAdapter, workersDynamo, workersCloudWatch);
    }

    @Bean
    public UploadValidationWorker uploadValidationWorker() {
        final String redisKey = uploadStatusRedisKey();
        final JedisOps jedisOps = jedisOps();
        return new UploadValidationWorker(redisKey, jedisOps);
    }
}
