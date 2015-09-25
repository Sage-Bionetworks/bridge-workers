package org.sagebionetworks.bridge.workers.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stream of a single DynamoDB table.
 */
public class DynamoStream {

    private final String tableName;
    private final String streamArn;

    public DynamoStream(final String tableName, final String streamArn) {
        checkNotNull(tableName);
        checkNotNull(streamArn);
        this.tableName = tableName;
        this.streamArn = streamArn;
    }

    /**
     * The fully qualified name of the DynamoDB table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Latest stream ARN.
     */
    public String getStreamArn() {
        return streamArn;
    }
}
