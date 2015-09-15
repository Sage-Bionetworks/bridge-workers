package org.sagebionetworks.bridge.workers.dynamodb;

import org.slf4j.Logger;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

abstract class StreamRecordProcessor implements IRecordProcessor {

    private final int checkpointInterval;

    StreamRecordProcessor(final int checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    @Override
    public void initialize(final InitializationInput initializationInput) {
        final ExtendedSequenceNumber seq = initializationInput.getExtendedSequenceNumber();
        getLogger().info("[ShardID=" + initializationInput.getShardId() + ", " +
                "SequenceNumber=" + seq.getSequenceNumber() + ", " +
                "SubSequenceNumber=" + seq.getSubSequenceNumber() + "]");
    }

    @Override
    public void processRecords(final ProcessRecordsInput processRecordsInput) {
        int count = 0;
        for (final Record record : processRecordsInput.getRecords()) {
            if(record instanceof RecordAdapter) {
                final com.amazonaws.services.dynamodbv2.model.Record streamRecord
                        = ((RecordAdapter) record).getInternalObject();
                getLogger().info("[EventId=" + streamRecord.getEventID() + ", " +
                        "SequenceNumber=" + streamRecord.getDynamodb().getSequenceNumber() + "]");
                process(streamRecord);
                count += 1;
                if (count % checkpointInterval == 0) {
                    try {
                        processRecordsInput.getCheckpointer().checkpoint();
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void shutdown(final ShutdownInput shutdownInput) {
        if(shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    abstract Logger getLogger();

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
                onModify(streamRecord);
                break;
            default:
                throw new RuntimeException(
                        "Unexpected event name: " + streamRecord.getEventName());
        }
    }
}
