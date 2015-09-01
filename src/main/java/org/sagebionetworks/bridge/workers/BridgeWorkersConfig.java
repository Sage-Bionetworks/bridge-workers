package org.sagebionetworks.bridge.workers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.Resource;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.google.common.base.StandardSystemProperty;

@Configuration
public class BridgeWorkersConfig {

    @Bean(name = "config")
    public Config config() {
        final ClassLoader classLoader = BridgeWorkersConfig.class.getClassLoader();
        final Path templateConfig = Paths.get(classLoader.getResource("workers.conf").getPath());
        final Path localConfig = Paths.get(StandardSystemProperty.USER_HOME.value() + "/.bridge/workers.conf");
        try {
            if (Files.exists(localConfig)) {
                return new PropertiesConfig(templateConfig, localConfig);
            }
            return new PropertiesConfig(templateConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean(name = "dynamo")
    @Resource(name = "config")
    public AmazonDynamoDBClient dynamo(Config workersConfig) {
        return new AmazonDynamoDBClient(new BasicAWSCredentials(
                workersConfig.get("aws.access.key"),
                workersConfig.get("aws.secret.key")));
    }

    @Bean(name = "dynamoStreams")
    @Resource(name = "config")
    public AmazonDynamoDBStreamsClient dynamoStreams(Config workersConfig) {
        return new AmazonDynamoDBStreamsClient(new BasicAWSCredentials(
                workersConfig.get("aws.access.key"),
                workersConfig.get("aws.secret.key")));
    }
}
