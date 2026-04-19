package com.hmdp.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Slf4j
@Primary
@Component
@RequiredArgsConstructor
public class FailoverIdWorker implements IdWorker {

    private final SnowflakeIdWorker snowflakeIdWorker;
    private final SegmentIdWorker segmentIdWorker;
    private final IdWorkerProperties properties;

    private volatile long segmentModeUntilMs = -1L;
    private volatile boolean segmentMode;

    @Override
    public long nextId(String keyPrefix) {
        long now = System.currentTimeMillis();
        if (segmentMode && now < segmentModeUntilMs) {
            return segmentIdWorker.nextId(keyPrefix);
        }

        try {
            long id = snowflakeIdWorker.nextId(keyPrefix);
            if (segmentMode) {
                segmentMode = false;
                log.info("Snowflake id generation recovered; switched back to snowflake mode");
            }
            return id;
        } catch (ClockRollbackException e) {
            long degradeDurationMs = properties.getFailover().getSegmentModeDurationMs();
            segmentModeUntilMs = Math.max(segmentModeUntilMs, now + degradeDurationMs);

            if (!segmentMode) {
                segmentMode = true;
                String level = e.getOffsetMs() >= properties.getSnowflake().getSevereRollbackThresholdMs()
                        ? "severe" : "moderate";
                log.warn("Detected {} clock rollback ({}ms); switch to segment mode for {}ms",
                        level, e.getOffsetMs(), degradeDurationMs);
            }
            return segmentIdWorker.nextId(keyPrefix);
        }
    }
}
