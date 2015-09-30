package org.sagebionetworks.bridge.workers.dynamodb.streams;

@SuppressWarnings("serial")
public class CheckpointException extends RuntimeException {

    public CheckpointException(Throwable cause) {
        super(cause);
    }
}
