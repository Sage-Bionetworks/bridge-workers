package org.sagebionetworks.bridge.workers;

import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.sagebionetworks.bridge.redis.JedisOps;

public class UploadValidationWorkerTest {

    @Test
    public void test() {
        final String redisKey = "redisKey";
        final String id1 = "id1";
        final String id2 = "id2";
        final Set<String> ids = new HashSet<>();
        ids.add(id1);
        ids.add(id2);
        final JedisOps jedisOps = mock(JedisOps.class);
        when(jedisOps.zrangeByScore(eq(redisKey), anyDouble(), anyDouble())).thenReturn(ids);
        UploadValidationWorker worker = new UploadValidationWorker(redisKey, jedisOps);
        worker.run();
        verify(jedisOps, times(1)).zrangeByScore(eq(redisKey), anyDouble(), anyDouble());
        verify(jedisOps, times(1)).zrem(eq(redisKey), anyString(), anyString());
    }
}
