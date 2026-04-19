package com.hmdp.utils;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Duration;
import java.util.UUID;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedisWorkerIdAssigner {

    private static final long MAX_WORKER_ID = 31L;

    private final StringRedisTemplate stringRedisTemplate;
    private final IdWorkerProperties properties;

    private final String instanceId = buildInstanceId();

    private volatile long workerId;
    private volatile String leaseKey;
    private volatile boolean redisLeaseActive;

    @PostConstruct
    public void init() {
        validateConfig();
        workerId = properties.getSnowflake().getFallbackWorkerId();
        refreshLease();
    }

    public long getWorkerId() {
        return workerId;
    }

    public long getDatacenterId() {
        return properties.getSnowflake().getDatacenterId();
    }

    @Scheduled(fixedDelayString = "${hmdp.id-worker.snowflake.worker-renew-interval-ms:10000}")
    public void maintainLease() {
        refreshLease();
    }

    @PreDestroy
    public synchronized void releaseLease() {
        if (!redisLeaseActive || leaseKey == null) {
            return;
        }
        try {
            String owner = stringRedisTemplate.opsForValue().get(leaseKey);
            if (instanceId.equals(owner)) {
                stringRedisTemplate.delete(leaseKey);
            }
        } catch (Exception e) {
            log.warn("Failed to release workerId lease, leaseKey={}", leaseKey, e);
        }
    }

    private synchronized void refreshLease() {
        if (redisLeaseActive && renewCurrentLease()) {
            return;
        }
        acquireLeaseOrFallback();
    }

    private boolean renewCurrentLease() {
        try {
            String owner = stringRedisTemplate.opsForValue().get(leaseKey);
            if (!instanceId.equals(owner)) {
                log.warn("WorkerId lease lost, leaseKey={}, owner={}", leaseKey, owner);
                redisLeaseActive = false;
                leaseKey = null;
                return false;
            }
            Boolean renewed = stringRedisTemplate.expire(
                    leaseKey,
                    Duration.ofSeconds(properties.getSnowflake().getWorkerLeaseSeconds()));
            if (Boolean.TRUE.equals(renewed)) {
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to renew workerId lease, leaseKey={}", leaseKey, e);
        }
        redisLeaseActive = false;
        leaseKey = null;
        return false;
    }

    private void acquireLeaseOrFallback() {
        Duration leaseDuration = Duration.ofSeconds(properties.getSnowflake().getWorkerLeaseSeconds());
        String keyPrefix = properties.getSnowflake().getWorkerLeaseKeyPrefix()
                + ":" + properties.getSnowflake().getDatacenterId() + ":";

        try {
            for (long candidate = 0; candidate <= MAX_WORKER_ID; candidate++) {
                String candidateKey = keyPrefix + candidate;
                Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(candidateKey, instanceId, leaseDuration);
                if (Boolean.TRUE.equals(success)) {
                    switchToRedisLease(candidate, candidateKey);
                    return;
                }

                String owner = stringRedisTemplate.opsForValue().get(candidateKey);
                if (instanceId.equals(owner)) {
                    stringRedisTemplate.expire(candidateKey, leaseDuration);
                    switchToRedisLease(candidate, candidateKey);
                    return;
                }
            }
            useFallbackWorkerId("No available workerId in Redis");
        } catch (Exception e) {
            useFallbackWorkerId("Failed to acquire workerId from Redis");
            log.warn("WorkerId allocation fell back to local config", e);
        }
    }

    private void switchToRedisLease(long leasedWorkerId, String leasedKey) {
        boolean changed = !redisLeaseActive || workerId != leasedWorkerId || !leasedKey.equals(leaseKey);
        workerId = leasedWorkerId;
        leaseKey = leasedKey;
        redisLeaseActive = true;
        if (changed) {
            log.info("WorkerId lease active, workerId={}, datacenterId={}, leaseKey={}",
                    workerId, getDatacenterId(), leaseKey);
        }
    }

    private void useFallbackWorkerId(String reason) {
        long fallbackWorkerId = properties.getSnowflake().getFallbackWorkerId();
        boolean changed = redisLeaseActive || workerId != fallbackWorkerId;
        workerId = fallbackWorkerId;
        leaseKey = null;
        redisLeaseActive = false;
        if (changed) {
            log.warn("{}; using fallback workerId={}, datacenterId={}",
                    reason, fallbackWorkerId, getDatacenterId());
        }
    }

    private void validateConfig() {
        long datacenterId = properties.getSnowflake().getDatacenterId();
        long fallbackWorkerId = properties.getSnowflake().getFallbackWorkerId();
        if (datacenterId < 0 || datacenterId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("datacenterId must be between 0 and " + MAX_WORKER_ID);
        }
        if (fallbackWorkerId < 0 || fallbackWorkerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("fallbackWorkerId must be between 0 and " + MAX_WORKER_ID);
        }
    }

    private String buildInstanceId() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            String pid = ManagementFactory.getRuntimeMXBean().getName();
            return host + "-" + pid + "-" + UUID.randomUUID();
        } catch (Exception e) {
            return "unknown-" + UUID.randomUUID();
        }
    }
}
