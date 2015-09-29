package org.sagebionetworks.bridge.workers.dynamodb.streams;

import java.util.List;

import org.slf4j.Logger;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

abstract class StreamRecordProcessor implements IRecordProcessor {

    private final int checkpointInterval;

    StreamRecordProcessor(final int checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    @Override
    public void initialize(final String shardId) {
        log().info("[ShardID=" + shardId + "]");
    }

    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer checkpointer) {
        int count = 0;
        for (final Record record : records) {
            if(record instanceof RecordAdapter) {
                final com.amazonaws.services.dynamodbv2.model.Record streamRecord
                        = ((RecordAdapter) record).getInternalObject();
                log().info("[EventId=" + streamRecord.getEventID() + ", " +
                        "SequenceNumber=" + streamRecord.getDynamodb().getSequenceNumber() + "]");
                process(streamRecord);
                count += 1;
                if (count % checkpointInterval == 0) {
                    try {
                        checkpointer.checkpoint();
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        if(reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    abstract Logger log();

    abstract void onInsert(com.amazonaws.services.dynamodbv2.model.Record streamRecord);

    abstract void onModify(com.amazonaws.services.dynamodbv2.model.Record streamRecord);

    abstract void onRemove(com.amazonaws.services.dynamodbv2.model.Record streamRecord);

    private void process(final com.amazonaws.services.dynamodbv2.model.Record streamRecord) {
        switch(streamRecord.getEventName()) {
            case "INSERT":
                onInsert(streamRecord);
                break;
            case "MODIFY":
                onModify(streamRecord);
                break;
            case "REMOVE":
                onRemove(streamRecord);
                break;
            default:
                throw new RuntimeException(
                        "Unexpected event name: " + streamRecord.getEventName());
        }
    }
}
