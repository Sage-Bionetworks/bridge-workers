package org.sagebionetworks.bridge.workers.dynamodb.streams;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;

public class ReplicaProcessorTest {

    @Test
    public void test() {
        ReplicaClient replicaClient = mock(ReplicaClient.class);
        ReplicaProcessor processor = new ReplicaProcessor(replicaClient);
        final StreamRecord streamRecord = mock(StreamRecord.class);
        final Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>();
        when(streamRecord.getNewImage()).thenReturn(newImage);
        final Map<String, AttributeValue> keys = new HashMap<String, AttributeValue>();
        when(streamRecord.getKeys()).thenReturn(keys);
        final Record record = mock(Record.class);
        when(record.getDynamodb()).thenReturn(streamRecord);
        processor.onInsert(record);
        verify(replicaClient, times(1)).putItem(newImage);
        processor.onModify(record);
        verify(replicaClient, times(2)).putItem(newImage);
        processor.onRemove(record);
        verify(replicaClient, times(1)).deleteItem(keys);
    }
}
