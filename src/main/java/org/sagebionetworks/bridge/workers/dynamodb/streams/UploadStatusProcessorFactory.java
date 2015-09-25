package org.sagebionetworks.bridge.workers.dynamodb.streams;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sagebionetworks.bridge.redis.JedisOps;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class UploadStatusProcessorFactory implements IRecordProcessorFactory {

    private final String redisKey;
    private final JedisOps jedisOps;

    public UploadStatusProcessorFactory(final String redisKey, final JedisOps jedisOps) {
        checkNotNull(redisKey);
        checkNotNull(jedisOps);
        this.redisKey = redisKey;
        this.jedisOps = jedisOps;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new UploadStatusProcessor(redisKey, jedisOps);
    }
}
