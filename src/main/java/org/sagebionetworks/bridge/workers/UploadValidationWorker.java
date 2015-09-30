package org.sagebionetworks.bridge.workers;

import java.util.Set;

import org.joda.time.DateTime;
import org.sagebionetworks.bridge.redis.JedisOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Periodically scans the Redis cache for uploads that are 1) more than 1 hour old
 * and 2) in REQUESTED or UNKNOWN status.
 *
 */
public class UploadValidationWorker {

    private final Logger log = LoggerFactory.getLogger(UploadValidationWorker.class);

    /** One hour in milliseconds. */
    private static final long UPLOAD_AGE_MILLIS = 60 * 60 * 1000;

    private final String redisKey;
    private final JedisOps jedisOps;

    public UploadValidationWorker(final String redisKey, final JedisOps jedisOps) {
        this.redisKey = redisKey;
        this.jedisOps = jedisOps;
    }

    @Async
    @Scheduled(fixedDelay = 10 * 60 * 1000)
    public void run() {
        final long now = DateTime.now().getMillis();
        final long ago = now - UPLOAD_AGE_MILLIS;
        final Set<String> uploadIds = jedisOps.zrangeByScore(redisKey, (double)ago, (double)now);
        log.info(uploadIds.size() + " uploads in REQUESTED or UNKNOWN status after an hour.");
        // TODO: For multiple workers, need to at least lock
        // TODO: Check S3 if the upload already exists. If it exists, call bridge server
        //       to trigger validation
        if (!uploadIds.isEmpty()) {
            jedisOps.zrem(redisKey, uploadIds.toArray(new String[uploadIds.size()]));
        }
    }
}
