package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.json.JSONObject;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Resource;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    
        // 本地 Caffeine 缓存，缓存 shop 对象，Key 为 shopId
        private final Cache<Long, Shop> localCache = Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();
    
    @Override
    public Result queryById(Long id) {
        // 1. 本地缓存优先
        Shop shop = localCache.getIfPresent(id);
        if (shop != null) {
            return Result.ok(shop);
        }

        // 2. 本地没有，走 Redis -> DB 的统一缓存读取逻辑（通过 CacheClient）
        shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("商铺不存在");
        }

        // 3. 将查询到的数据放入本地缓存
        localCache.put(id, shop);
        return Result.ok(shop);
    }

    // private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // public Shop queryWithLogicalExpire(Long id) {
    //     // 1.从redis中查询商户缓存
    //     String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
    //     // 2.判断商户缓存是否存在
    //     if (StrUtil.isBlank(shopJson)) {
    //         return null;
    //     }
    //     // 3.存在，判断是否过期
    //     RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
    //     Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
    //     if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
    //         // 4.未过期，直接返回商户信息
    //         return shop;
    //     }

    //     // 5.过期，缓存重建
    //         // 5.1获取互斥锁
    //     String lockKey = LOCK_SHOP_KEY + id;
    //     boolean isLock = tryLock(lockKey);
    //     // 5.2判断是否获取成功
    //     if (isLock) {
    //     // 5.3成功，开启独立线程，实现缓存重建
    //         CACHE_REBUILD_EXECUTOR.submit(() -> {
    //             try {
    //                 // 重建缓存
    //                 this.saveShopToRedis(id, 20L);
    //             } catch (Exception e) {
    //                 throw new RuntimeException(e);
    //             } finally {
    //                 // 释放锁
    //                 unLock(lockKey);
    //             }
    //         });
    //     }
    //     return shop;
    // }

    // 将商户信息保存到redis中
    // public void saveShopToRedis(Long id, Long expireSeconds) throws InterruptedException {
    //     // 1.查询数据库
    //     Shop shop = getById(id);
    //     Thread.sleep(200);
    //     // 2.封装逻辑过期时间
    //     RedisData redisData = new RedisData();
    //     redisData.setData(shop);
    //     redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
    //     // 3.写入redis
    //     stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    // }

    // public Shop queryWithMutex(Long id) {
    //     // 1.从redis中查询商户缓存
    //     String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
    //     // 2.判断商户缓存是否存在
    //     if (StrUtil.isNotBlank(shopJson)) {
    //         // 3.存在，直接返回
    //         Shop shop = JSONUtil.toBean(shopJson, Shop.class);
    //         return shop;
    //     }
    //     //判断商户缓存是否命中空字符串
    //     if (shopJson != null) {
    //         return null;
    //     }
    //     // 4.实现缓存重建
    //     // 4.1获取互斥锁
    //     String lockKey = LOCK_SHOP_KEY + id;
    //     Shop shop = null;
    //     try {
    //         boolean isLock = tryLock(lockKey);
    //         // 4.2判断是否获取成功
    //         if (!isLock) {
    //             // 4.3失败，休眠并重试
    //             Thread.sleep(50);
    //             return queryWithMutex(id);
    //         }
    //         // 4.4成功，根据id查询数据库
    //         shop = getById(id);
    //         // 5.数据库中不存在，返回错误信息
    //         if (shop == null) {
    //             stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
    //             return null;
    //         }
    //         // 6.数据库中存在，将数据写入redis
    //         stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), LOCK_SHOP_TTL, TimeUnit.MINUTES);
    //     } catch (InterruptedException e) {
    //         throw new RuntimeException(e);
    //     } finally {
    //         // 7.释放互斥锁
    //         unLock(lockKey);
    //     }
    //     return shop;
    // }

    // public Shop queryWithPassThrough(Long id) {
    //     // 1.从redis中查询商户缓存
    //     String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
    //     // 2.判断商户缓存是否存在
    //     if (StrUtil.isNotBlank(shopJson)) {
    //         // 3.存在，直接返回
    //         Shop shop = JSONUtil.toBean(shopJson, Shop.class);
    //         return shop;
    //     }
    //     //判断商户缓存是否命中空字符串
    //     if (shopJson != null) {
    //         return null;
    //     }
    //     // 4.不存在，根据id查询数据库
    //     Shop shop = getById(id);
    //     // 5.数据库中不存在，返回错误信息
    //     if (shop == null) {
    //         stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
    //         return null;
    //     }
    //     // 6.数据库中存在，将数据写入redis
    //     stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
    //     return shop;

    // }

    // private boolean tryLock(String key) {
    //     Boolean lock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
    //     return Boolean.TRUE.equals(lock);
    // }

    // private void unLock(String key) {
    //     stringRedisTemplate.delete(key);
    // }

    @Override
    @Transactional
    public Result update(Shop shop){
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺ID不能为空");
        }
        updateById(shop);
        // 删除 Redis 缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        // 同步删除本地缓存
        localCache.invalidate(id);
        return Result.ok();
    }
}
