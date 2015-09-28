package org.sagebionetworks.bridge.workers.dynamodb.streams;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.dynamodb.DynamoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;

public final class StreamsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsUtils.class);

    private static final Long MIN_READ_THROUGHPUT = 5L;

    /**
     * The list of all the existing tables for the current environment and user.
     * This is a list of fully qualified table names.
     */
    public static List<String> getTables(final Config config, final AmazonDynamoDB dynamo) {
        checkNotNull(config);
        checkNotNull(dynamo);
        final Map<String, TableDescription> tables = DynamoUtils.getExistingTables(config, dynamo);
        return new ArrayList<String>(tables.keySet());
    }

    /**
     * The list of tables that exist for the current environment and user. Only tables in the specified
     * list are included if they indeed exist. This is a list of fully qualified table names.
     */
    public static List<String> getTables(final List<String> simpleTableNames, final Config config,
            final AmazonDynamoDB dynamo) {
        checkNotNull(simpleTableNames);
        checkNotNull(config);
        checkNotNull(dynamo);
        final List<String> fqTableNames = simpleTableNames.stream()
                .map(simpleTableName -> DynamoUtils.getFullyQualifiedTableName(simpleTableName, config))
                .collect(Collectors.toList());
        final Map<String, TableDescription> allTables = DynamoUtils.getExistingTables(config, dynamo);
        return fqTableNames.stream()
                .filter(fqTableName -> allTables.containsKey(fqTableName))
                .collect(Collectors.toList());
    }

    public static List<DynamoStream> getStreams(final List<String> fqTableNames, final AmazonDynamoDB dynamo) {
        checkNotNull(fqTableNames);
        checkNotNull(dynamo);
        final List<DynamoStream> streams = new ArrayList<>();
        for (final String fqTableName : fqTableNames) {
            if (!isStreamEnabled(fqTableName, dynamo)) {
                LOG.info("Enabling stream for table " + fqTableName + "...");
                enableStream(fqTableName, dynamo);
                LOG.info("Stream enabled for table " + fqTableName + "... Done.");
            }
            final DescribeTableResult describeTableResult = dynamo.describeTable(fqTableName);
            streams.add(new DynamoStream(fqTableName, describeTableResult.getTable().getLatestStreamArn()));
        }
        return streams;
    }

    public static List<String> getReplicaTables(final List<String> streamsTables,
            final AmazonDynamoDB streamsDynamo, final AmazonDynamoDB replicaDynamo) {
        checkNotNull(streamsTables);
        checkNotNull(streamsDynamo);
        checkNotNull(replicaDynamo);
        final Map<String, TableDescription> replicaTables = DynamoUtils.getExistingTables(replicaDynamo);
        for (final String streamTable : streamsTables) {
            final TableDescription streamTableDescription = streamsDynamo.describeTable(streamTable).getTable();
            if (replicaTables.containsKey(streamTable)) {
                DynamoUtils.compareKeySchema(streamTableDescription, replicaTables.get(streamTable));
            } else {
                final String tableName = streamTableDescription.getTableName();
                final List<KeySchemaElement> keys = streamTableDescription.getKeySchema();
                final Set<String> keyNames = keys.stream().map(key -> key.getAttributeName()).collect(Collectors.toSet());
                // Remove non-key attributes -- the list of attributes must align with the key schema
                final List<AttributeDefinition> attributes = streamTableDescription.getAttributeDefinitions().stream()
                        .filter(attr -> keyNames.contains(attr.getAttributeName())).collect(Collectors.toList());
                final CreateTableRequest createTableRequest = new CreateTableRequest()
                        .withTableName(tableName).withKeySchema(keys).withAttributeDefinitions(attributes);
                final ProvisionedThroughput throughput = new ProvisionedThroughput(MIN_READ_THROUGHPUT,
                        streamTableDescription.getProvisionedThroughput().getWriteCapacityUnits());
                createTableRequest.setProvisionedThroughput(throughput);
                LOG.info("Creating replica table " + tableName + "...");
                replicaDynamo.createTable(createTableRequest);
                LOG.info("Creating replica table " + tableName + "... Done.");
                DynamoUtils.waitForActive(replicaDynamo, streamTable);
            }
        }
        return streamsTables;
    }

    private static void enableStream(final String fqTableName, final AmazonDynamoDB dynamo) {
        checkNotNull(fqTableName);
        checkNotNull(dynamo);
        final StreamSpecification streamSpecification = new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        final UpdateTableRequest request = new UpdateTableRequest()
            .withTableName(fqTableName)
            .withStreamSpecification(streamSpecification);
        dynamo.updateTable(request);
        while(!isStreamEnabled(fqTableName, dynamo)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException("Shouldn't be interrupted here.");
            }
        }
    }

    private static boolean isStreamEnabled(final String fqTableName, final AmazonDynamoDB dynamo) {
        final DescribeTableResult describeTableResult = dynamo.describeTable(fqTableName);
        final StreamSpecification streamSpecification = describeTableResult.getTable().getStreamSpecification();
        return streamSpecification != null && streamSpecification.isStreamEnabled();
    }

    private StreamsUtils() {}
}
