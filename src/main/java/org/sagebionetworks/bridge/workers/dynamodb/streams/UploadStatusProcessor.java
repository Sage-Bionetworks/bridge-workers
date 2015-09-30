package org.sagebionetworks.bridge.workers.dynamodb.streams;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sagebionetworks.bridge.redis.JedisOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.Record;

public class UploadStatusProcessor extends StreamRecordProcessor {

    private final Logger log = LoggerFactory.getLogger(UploadStatusProcessor.class);

    private final String redisKey;
    private final JedisOps jedisOps;

    public UploadStatusProcessor(final String redisKey, final JedisOps jedisOps) {
        super(20);
        checkNotNull(redisKey);
        checkNotNull(jedisOps);
        this.redisKey = redisKey;
        this.jedisOps = jedisOps;
    }

    @Override
    Logger log() {
        return log;
    }

    @Override
    void onInsert(final Record record) {
        if (isRequestedOrUnknown(record)) {
            final String uploadId = getUploadId(record);
            final double score = DateTime.now(DateTimeZone.UTC).getMillis();
            jedisOps.zadd(redisKey, score, uploadId);
        }
    }

    @Override
    void onModify(final Record record) {
        if (!isRequestedOrUnknown(record)) {
            jedisOps.zrem(redisKey, getUploadId(record));
        }
    }

    @Override
    void onRemove(final Record record) {
        jedisOps.zrem(redisKey, getUploadId(record));
    }

    /**
     * Whether the upload status is "REQUESTED" or "UNKNOWN".
     */
    private boolean isRequestedOrUnknown(final Record record) {
        final String status = record.getDynamodb().getNewImage().get("status").getS();
        return "UNKNOWN".equals(status) || "REQUESTED".equals(status);
    }

    private String getUploadId(final Record record) {
        return record.getDynamodb().getNewImage().get("uploadId").getS();
    }
}
