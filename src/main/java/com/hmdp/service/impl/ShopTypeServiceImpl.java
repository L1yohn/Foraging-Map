package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    public Result queryTypeList() {
        String key = "cache:shopType:list";
        String json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(json)){
            List<ShopType> shopTypeList = JSONUtil.toList(json, ShopType.class);
            return Result.ok(shopTypeList);
        }
        List<ShopType> typeList  = query().orderByAsc("sort").list();
        if (typeList  == null || typeList .isEmpty()) {
            return Result.fail("商铺类型不存在");
        }
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList),30, TimeUnit.MINUTES);
        return Result.ok(typeList);
    }
}

