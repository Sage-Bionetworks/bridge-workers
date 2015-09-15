package org.sagebionetworks.bridge.workers.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.Record;

public class UploadStatusProcessor extends StreamRecordProcessor {

    private final Logger logger = LoggerFactory.getLogger(UploadStatusProcessor.class);

    public UploadStatusProcessor() {
        super(20);
    }

    @Override
    Logger getLogger() {
        return logger;
    }

    @Override
    void onInsert(final Record record) {
        addUpload(record);
    }

    @Override
    void onModify(final Record record) {
        removeUpload(record);
    }

    @Override
    void onRemove(final Record record) {
        removeUpload(record);
    }

    private void addUpload(final Record record) {
        // TODO
    }

    private void removeUpload(final Record record) {
        // TODO
    }
}
