package com.hmdp.utils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.annotation.PostConstruct;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.redisson.api.RKeys;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于 Redisson 的布隆过滤器实现（使用 Redis 存储）
 * - 启动时全量构建
 * - 新增数据时同步调用 `put`
 * - 删除通过每天定时重建（双缓冲）实现
 * - 出错或不可用时降级放行到 Redis 层
 */
@Component
@Slf4j
public class BloomFilterHelper {

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private ShopMapper shopMapper;

    // 原子引用当前生效的布隆过滤器名称
    private final AtomicReference<String> currentFilterName = new AtomicReference<>();

    // 标记是否可用
    private volatile boolean available = false;

    // 误判率
    private static final double FPP = 0.01;

    @PostConstruct
    public void init() {
        try {
            rebuildFilter();
            available = true;
            log.info("Redisson BloomFilter initialized");
        } catch (Exception e) {
            available = false;
            log.error("Failed to initialize Redisson BloomFilter", e);
        }
    }

    // 每天凌晨重建一次（双缓冲），构建完成后原子切换
    @Scheduled(cron = "0 0 0 * * ?")
    public void scheduledRebuild() {
        log.info("Starting scheduled Redisson BloomFilter rebuild");
        try {
            rebuildFilter();
            available = true;
            log.info("Redisson BloomFilter rebuild finished");
        } catch (Exception e) {
            available = false;
            log.error("Redisson BloomFilter rebuild failed", e);
        }
    }

    private void rebuildFilter() {
        List<Shop> shops = shopMapper.selectList(null);
        int expectedInsertions = Math.max(1000, shops == null ? 0 : shops.size());

        String newName = "bloom:shop:" + UUID.randomUUID().toString();
        RBloomFilter<Long> newFilter = redissonClient.getBloomFilter(newName);
        boolean inited = newFilter.tryInit(expectedInsertions, FPP);
        if (!inited) {
            log.warn("RBloomFilter tryInit returned false for {}", newName);
        }
        if (shops != null) {
            for (Shop s : shops) {
                if (s != null && s.getId() != null) {
                    newFilter.add(s.getId());
                }
            }
        }
        // 原子切换 name
        String old = currentFilterName.getAndSet(newName);
        // 删除旧过滤器的 Redis key（异步/尽力删除）
        if (old != null) {
            try {
                RKeys keys = redissonClient.getKeys();
                keys.delete(old);
            } catch (Exception e) {
                log.warn("Failed to delete old bloom filter key {}", old, e);
            }
        }
    }

    /**
     * 布隆检查（布隆不可用时降级返回 true，放行到 Redis 层）
     */
    public boolean mightContain(Long id) {
        if (id == null) return true;
        try {
            String name = currentFilterName.get();
            if (name == null) return true; // 降级放行
            RBloomFilter<Long> filter = redissonClient.getBloomFilter(name);
            return filter.contains(id);
        } catch (Exception e) {
            log.error("Redisson BloomFilter mightContain error", e);
            return true;
        }
    }

    public boolean isAvailable() {
        return available && currentFilterName.get() != null;
    }

    // 新增时，将 id 添加到当前生效的过滤器（若可用）
    public void put(Long id) {
        if (id == null) return;
        try {
            String name = currentFilterName.get();
            if (name == null) return;
            RBloomFilter<Long> filter = redissonClient.getBloomFilter(name);
            filter.add(id);
        } catch (Exception e) {
            log.error("Redisson BloomFilter put error", e);
        }
    }
}
