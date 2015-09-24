package org.sagebionetworks.bridge.workers.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sagebionetworks.bridge.redis.JedisOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.Record;

public class UploadStatusProcessor extends StreamRecordProcessor {

    private final Logger logger = LoggerFactory.getLogger(UploadStatusProcessor.class);

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
        final String status = record.getDynamodb().getNewImage().get("status").getS();
        if ("UNKNOWN".equals(status) || "REQUESTED".equals(status)) {
            final String uploadId = record.getDynamodb().getNewImage().get("uploadId").getS();
            final double score = DateTime.now(DateTimeZone.UTC).getMillis();
            jedisOps.zadd(redisKey, score, uploadId);
        }
    }

    private void removeUpload(final Record record) {
        final String status = record.getDynamodb().getNewImage().get("status").getS();
        if (!"UNKNOWN".equals(status) && !"REQUESTED".equals(status)) {
            final String uploadId = record.getDynamodb().getNewImage().get("uploadId").getS();
            jedisOps.zrem(redisKey, uploadId);
        }
    }
}
