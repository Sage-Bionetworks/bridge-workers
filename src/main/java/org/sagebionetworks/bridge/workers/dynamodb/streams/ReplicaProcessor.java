package org.sagebionetworks.bridge.workers.dynamodb.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.Record;

public class ReplicaProcessor extends StreamRecordProcessor {

    private final Logger log = LoggerFactory.getLogger(ReplicaProcessor.class);

    private final ReplicaClient replicaClient;

    public ReplicaProcessor(final ReplicaClient replicaClient) {
        super(100);
        this.replicaClient = replicaClient;
    }

    @Override
    Logger log() {
        return log;
    }

    @Override
    void onInsert(Record streamRecord) {
        replicaClient.putItem(streamRecord.getDynamodb().getNewImage());
    }

    @Override
    void onModify(Record streamRecord) {
        replicaClient.putItem(streamRecord.getDynamodb().getNewImage());
    }

    @Override
    void onRemove(Record streamRecord) {
        replicaClient.deleteItem(streamRecord.getDynamodb().getKeys());
    }
}
