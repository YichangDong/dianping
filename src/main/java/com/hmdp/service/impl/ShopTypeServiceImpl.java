package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.util.List;

import jakarta.annotation.Resource;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryType() {
        // 1.查询商户类型缓存
        String typeList = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        // 2.判断商户类型缓存是否存在
        if (StrUtil.isNotBlank(typeList)) {
            // 3.存在，直接返回
            List<ShopType> list = JSONUtil.toList(typeList, ShopType.class);
            return Result.ok(list);
        }
        // 4.不存在，查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();

        // 5.数据库中不存在，返回错误信息
        if (shopTypeList == null || shopTypeList.isEmpty()) {
            return Result.fail("商户类型不存在");
        }
        // 6.数据库中存在，将数据写入redis        
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypeList));
        return Result.ok(shopTypeList);
    }


    
}
