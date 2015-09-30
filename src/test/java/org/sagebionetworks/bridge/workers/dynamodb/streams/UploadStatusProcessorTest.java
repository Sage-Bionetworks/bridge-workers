package org.sagebionetworks.bridge.workers.dynamodb.streams;

import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.sagebionetworks.bridge.redis.JedisOps;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;

public class UploadStatusProcessorTest {

    @Test
    public void testInsert() {
        final String redisKey = "redisKey";
        final JedisOps jedisOps = mock(JedisOps.class);
        UploadStatusProcessor processor = new UploadStatusProcessor(redisKey, jedisOps);
        processor.onInsert(mockRecord("REQUESTED", "0"));
        processor.onInsert(mockRecord("UNKNOWN", "1"));
        processor.onInsert(mockRecord("SUCCEEDED", "2"));
        verify(jedisOps, times(1)).zadd(eq(redisKey), anyDouble(), eq("0"));
        verify(jedisOps, times(1)).zadd(eq(redisKey), anyDouble(), eq("1"));
        verify(jedisOps, never()).zadd(eq(redisKey), anyDouble(), eq("2"));
    }

    @Test
    public void testModify() {
        final String redisKey = "redisKey";
        final JedisOps jedisOps = mock(JedisOps.class);
        UploadStatusProcessor processor = new UploadStatusProcessor(redisKey, jedisOps);
        processor.onModify(mockRecord("REQUESTED", "0"));
        processor.onModify(mockRecord("UNKNOWN", "1"));
        processor.onModify(mockRecord("SUCCEEDED", "2"));
        verify(jedisOps, never()).zrem(eq(redisKey), eq("0"));
        verify(jedisOps, never()).zrem(eq(redisKey), eq("1"));
        verify(jedisOps, times(1)).zrem(eq(redisKey), eq("2"));
    }

    @Test
    public void testRemove() {
        final String redisKey = "redisKey";
        final JedisOps jedisOps = mock(JedisOps.class);
        UploadStatusProcessor processor = new UploadStatusProcessor(redisKey, jedisOps);
        processor.onRemove(mockRecord("REQUESTED", "0"));
        processor.onRemove(mockRecord("UNKNOWN", "1"));
        processor.onRemove(mockRecord("SUCCEEDED", "2"));
        verify(jedisOps, times(1)).zrem(eq(redisKey), eq("0"));
        verify(jedisOps, times(1)).zrem(eq(redisKey), eq("1"));
        verify(jedisOps, times(1)).zrem(eq(redisKey), eq("2"));
    }

    private Record mockRecord(final String status, final String uploadId) {
        Map<String, AttributeValue> image = new HashMap<>();
        AttributeValue attrStatus = mock(AttributeValue.class);
        when(attrStatus.getS()).thenReturn(status);
        image.put("status", attrStatus);
        AttributeValue attrUploadId = mock(AttributeValue.class);
        when(attrUploadId.getS()).thenReturn(uploadId);
        image.put("uploadId", attrUploadId);
        StreamRecord streamRecord = mock(StreamRecord.class);
        when(streamRecord.getNewImage()).thenReturn(image);
        Record record = mock(Record.class);
        when(record.getDynamodb()).thenReturn(streamRecord);
        return record;
    }
}
