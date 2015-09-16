package org.sagebionetworks.bridge.workers.dynamodb;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class ReplicaClient {

    private final String table;
    private final AmazonDynamoDB dynamo;

    public ReplicaClient(final String table, final AmazonDynamoDB dynamo) {
        this.table = table;
        this.dynamo = dynamo;
    }

    public void putItem(final Map<String, AttributeValue> item) {
        dynamo.putItem(table, item);
    }

    public void deleteItem(final Map<String, AttributeValue> key) {
        dynamo.deleteItem(table, key);
    }
}
