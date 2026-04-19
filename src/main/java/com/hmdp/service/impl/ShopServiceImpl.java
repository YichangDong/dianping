package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.BloomFilterHelper;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.json.JSONObject;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.SystemConstants;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.hash.BloomFilter;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
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
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private BloomFilterHelper bloomFilterHelper;
    
            // 本地 Caffeine 缓存，缓存 shop 对象，Key 为 shopId
            // 最大容量 10000 条，写入后过期 10 分钟，访问后过期 5 分钟
            private final Cache<Long, Shop> localCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterAccess(5, TimeUnit.MINUTES)
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

        @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
    /**
     * 新增店铺时，同步添加到布隆过滤器
     */
    @Transactional
    @Override
    public Result saveShop(Shop shop) {
        // 保存到数据库
        save(shop);

        // 添加到布隆过滤器
        bloomFilterHelper.put(shop.getId());
        log.info("新店铺 {} 已添加到布隆过滤器", shop.getId());

        return Result.ok(shop.getId());
    }
}
