package com.hmdp.utils;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.id-worker")
public class IdWorkerProperties {

    private Snowflake snowflake = new Snowflake();

    private Segment segment = new Segment();

    private Failover failover = new Failover();

    @Data
    public static class Snowflake {
        private long beginTimestamp = 1640995200000L;
        private long datacenterId = 1L;
        private long fallbackWorkerId = 1L;
        private long smallRollbackThresholdMs = 5L;
        private long severeRollbackThresholdMs = 5000L;
        private int workerLeaseSeconds = 30;
        private long workerRenewIntervalMs = 10000L;
        private String workerLeaseKeyPrefix = "hmdp:id:worker";
    }

    @Data
    public static class Segment {
        private long step = 1000L;
        private String redisKeyPrefix = "hmdp:id:segment";
    }

    @Data
    public static class Failover {
        private long segmentModeDurationMs = 30000L;
    }
}
