package org.sagebionetworks.bridge.workers.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class ReplicaProcessorFactory implements IRecordProcessorFactory {

    private final ReplicaClient replicaClient;

    public ReplicaProcessorFactory(final String table, final AmazonDynamoDB dynamo) {
        replicaClient = new ReplicaClient(table, dynamo);
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new ReplicaProcessor(replicaClient);
    }
}
