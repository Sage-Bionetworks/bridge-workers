package org.sagebionetworks.bridge.workers.dynamodb.streams;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * The client dedicated to update a specific replica table.
 */
class ReplicaClient {

    private final String table;
    private final AmazonDynamoDB dynamo;

    ReplicaClient(final String replicaTable, final AmazonDynamoDB replicaDynamo) {
        this.table = replicaTable;
        this.dynamo = replicaDynamo;
    }

    public void putItem(final Map<String, AttributeValue> item) {
        dynamo.putItem(table, item);
    }

    public void deleteItem(final Map<String, AttributeValue> key) {
        dynamo.deleteItem(table, key);
    }
}
