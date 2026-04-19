package com.hmdp.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SegmentIdWorker {

    private final StringRedisTemplate stringRedisTemplate;
    private final IdWorkerProperties properties;

    private final ConcurrentMap<String, SegmentBuffer> buffers = new ConcurrentHashMap<>();

    public long nextId(String keyPrefix) {
        SegmentBuffer buffer = buffers.computeIfAbsent(keyPrefix, ignored -> new SegmentBuffer());
        long rawId = nextRawId(buffer, keyPrefix);
        return (rawId << 1) | 1L;
    }

    private long nextRawId(SegmentBuffer buffer, String keyPrefix) {
        while (true) {
            long candidate = buffer.current.getAndIncrement();
            if (candidate <= buffer.maxId) {
                return candidate;
            }

            synchronized (buffer) {
                if (buffer.current.get() - 1 <= buffer.maxId) {
                    continue;
                }
                allocateSegment(buffer, keyPrefix);
            }
        }
    }

    private void allocateSegment(SegmentBuffer buffer, String keyPrefix) {
        long step = properties.getSegment().getStep();
        Long newMax = stringRedisTemplate.opsForValue().increment(segmentKey(keyPrefix), step);
        if (newMax == null) {
            throw new IllegalStateException("Failed to allocate segment for keyPrefix=" + keyPrefix);
        }
        long start = newMax - step + 1;
        buffer.current.set(start);
        buffer.maxId = newMax;
    }

    private String segmentKey(String keyPrefix) {
        return properties.getSegment().getRedisKeyPrefix() + ":" + keyPrefix;
    }

    private static final class SegmentBuffer {
        private final AtomicLong current = new AtomicLong(0L);
        private volatile long maxId = -1L;
    }
}
