package com.hmdp.utils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class CacheClient {


    private final StringRedisTemplate stringRedisTemplate;
    @Resource
    private BloomFilterHelper bloomFilterHelper;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }


    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }


    public <T, ID> T queryWithPassThrough(String keyprefix,ID id, Class<T> type, Function<ID, T> dbFallback, Long time, TimeUnit unit) {
        String key = keyprefix + id;
        // 1.从redis中查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断商户缓存是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断缓存是否命中空字符串
        if (json != null) {
            // json 为空字符串表示此前缓存了空值；这里我们已改用布隆过滤器来防穿透，仍保留兼容逻辑：
            if (json.length() == 0) {
                return null;
            }
        }
        // 4.不存在，根据id查询数据库
        // 在查询数据库前，先用布隆过滤器判断是否可能存在，若不可能则直接返回 null，避免穿透
        if (bloomFilterHelper != null && bloomFilterHelper.isAvailable() && id instanceof Long) {
            Long lid = (Long) id;
            if (!bloomFilterHelper.mightContain(lid)) {
                return null;
            }
        }

        T t = dbFallback.apply(id);
        if(t == null) {
            // 5.数据库中不存在，使用布隆过滤器已进行了可能性判断，不再写入空字符串到 Redis
            return null;
        }
        // 6.数据库中存在，写入redis，设置过期时间
        this.set(key, t, time, unit);
        return t;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <T, ID> T queryWithLogicalExpire(String keyprefix,ID id, Class<T> type, Function<ID, T> dbFallback, Long time, TimeUnit unit) {
        String key = keyprefix + id;
        // 1.从redis中查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断商户缓存是否存在
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 3.存在，判断是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        T t = JSONUtil.toBean((String) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 4.未过期，直接返回店铺信息
            return t;
        }
        // 5.已过期，需要缓存重建
        // 5.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 5.2.判断是否获取锁成功
        if (isLock) {
            // 5.3.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    T newT = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, newT, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });
        }
        return t;
    }



    private boolean tryLock(String key) {
        Boolean lock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return Boolean.TRUE.equals(lock);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
