package org.sagebionetworks.bridge.workers.dynamodb;

import java.util.Collections;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

/**
 * Responsible for listing the tables that will have replicas.
 */
public class ReplicaStreams {

    private final List<DynamoStream> streams;

    public ReplicaStreams(final List<String> fqTableNames, final AmazonDynamoDB dynamo) {
        streams = Collections.unmodifiableList(ReplicaUtils.getStreams(fqTableNames, dynamo));
    }

    public List<DynamoStream> getStreams() {
        return streams;
    }
}
