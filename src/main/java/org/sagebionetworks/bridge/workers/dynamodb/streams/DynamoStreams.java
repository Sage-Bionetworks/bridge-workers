package org.sagebionetworks.bridge.workers.dynamodb.streams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

/**
 * Responsible for listing the tables that will have replicas.
 */
public class DynamoStreams {

    private final Map<String, DynamoStream> streams;

    public DynamoStreams(final List<String> fqTableNames, final AmazonDynamoDB dynamo) {
        streams = new HashMap<>();
        StreamsUtils.getStreams(fqTableNames, dynamo).stream().forEach(
                stream -> streams.put(stream.getTableName(), stream));
    }

    public List<DynamoStream> getStreams() {
        return Collections.unmodifiableList(new ArrayList<DynamoStream>(streams.values()));
    }

    public DynamoStream getStream(final String fqTableName) {
        return streams.get(fqTableName);
    }
}
