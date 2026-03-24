package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿
        Shop shop = quertWithMutex(id);
        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }
    public Shop quertWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.命中有效缓存，直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 3.命中空值缓存，直接返回不存在，避免缓存穿透
        if (shopJson != null) {
            return null;
        }
        //获取互斥锁
        String lockKey=CACHE_SHOP_KEY+id;
        boolean islock = trylock(lockKey);
        //判断时候成功
        if (!islock) {
            //失败休眠重试
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return quertWithMutex(id);
        }
        // 4.未命中，根据id查询数据库
        Shop shop = getById(id);
        // 5.数据库中不存在，写入空值缓存
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.数据库中存在，写入缓存并设置TTL
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //释放锁
        unLock(lockKey);
        return shop;
    }
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.命中有效缓存，直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 3.命中空值缓存，直接返回不存在，避免缓存穿透
        if (shopJson != null) {
            return null;
        }
        // 4.未命中，根据id查询数据库
        Shop shop = getById(id);
        // 5.数据库中不存在，写入空值缓存
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.数据库中存在，写入缓存并设置TTL
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }
        private boolean trylock(String key) {
            Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
            return BooleanUtil.isTrue(flag);
        }
        private void unLock(String key) {
            stringRedisTemplate.delete(key);
        }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("商铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }
}
