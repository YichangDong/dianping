package com.hmdp.utils;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SnowflakeIdWorker {

    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    private final IdWorkerProperties properties;
    private final RedisWorkerIdAssigner workerIdAssigner;

    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public synchronized long nextId(String keyPrefix) {
        long timestamp = currentTimestamp();
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            if (offset <= properties.getSnowflake().getSmallRollbackThresholdMs()) {
                timestamp = waitUntil(lastTimestamp);
            } else {
                throw new ClockRollbackException(offset);
            }
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                timestamp = waitUntil(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;
        long rawId = ((timestamp - properties.getSnowflake().getBeginTimestamp()) << TIMESTAMP_SHIFT)
                | (workerIdAssigner.getDatacenterId() << DATACENTER_ID_SHIFT)
                | (workerIdAssigner.getWorkerId() << WORKER_ID_SHIFT)
                | sequence;
        return rawId << 1;
    }

    private long waitUntil(long targetTimestamp) {
        long timestamp = currentTimestamp();
        while (timestamp <= targetTimestamp) {
            Thread.onSpinWait();
            timestamp = currentTimestamp();
        }
        return timestamp;
    }

    private long currentTimestamp() {
        return System.currentTimeMillis();
    }
}
