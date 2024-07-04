package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.ReactiveGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺信息 并存入缓存
     *
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透
        // Shop shop = queryWithPassThrough(id);

        // 利用互斥锁解决缓存击穿
        // Shop shop = queryWithMutex(id);

        // 利用逻辑过期时间解决缓存击穿
        // Shop shop = queryWithLogicalExpire(id);

        // 利用工具类解决缓存穿透问题
        // Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 利用工具类的逻辑过期时间方法解决缓存击穿问题
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, LOCK_SHOP_KEY, 20L, TimeUnit.SECONDS);  // 设置20s方便测试缓存击穿

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    /**
     * 解决缓存穿透 -- 客户端请求的数据在缓存和数据库当中都不存在，这样缓存永远不会生效，请求全部被打到数据库当中
     *
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        // 1、从redis中查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        Map<Object, Object> shopMap = stringRedisTemplate.opsForHash().entries(key);

        // 2、判断缓存是否存在
        if (!shopMap.isEmpty()) {
            // 解决缓存穿透 -- 判断缓存命中的是否是空值
            if (shopMap.containsKey("")) {
                // 返回错误
                return null;
            }

            // 3、如果存在则直接返回数据结果
            return BeanUtil.fillBeanWithMap(shopMap, new Shop(), false);
        }

        // 4、若不存在则根据id查询数据库
        Shop shop = getById(id);

        // 5、数据库中不存在数据则返回错误（请求无法查找到数据）
        if (shop == null) {
            // 解决缓存穿透问题，当数据库中不存在该数据时将空值写入redis缓存
            HashMap<Object, Object> blankMap = new HashMap<>();
            blankMap.put("", "");

            stringRedisTemplate.opsForHash().putAll(key, blankMap);
            stringRedisTemplate.expire(key, CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }

        // 6、数据库存在，则先将数据写入redis缓存
        Map<String, Object> shopMapToCache = BeanUtil.beanToMap(shop, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)  // 实际上不起作用，所以我在后面处理了fieldName为空的情况
                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue == null ? null : fieldValue.toString()));
        stringRedisTemplate.opsForHash().putAll(key, shopMapToCache);
        stringRedisTemplate.expire(key, CACHE_SHOP_TTL, TimeUnit.MINUTES);  // 追加设置超时时间

        return shop;
    }

    /**
     * 利用互斥锁解决缓存击穿
     *
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) {
        // 1、从redis中查询商铺缓存
        String key = CACHE_SHOP_KEY + id.toString();
        Map<Object, Object> shopMap = stringRedisTemplate.opsForHash().entries(key);

        // 2、判断缓存是否存在
        if (!shopMap.isEmpty()) {
            // 解决缓存穿透 -- 判断缓存命中的是否是空值
            if (shopMap.containsKey("")) {
                // 返回错误
                return null;
            }

            // 3、如果存在则直接返回数据结果
            return BeanUtil.fillBeanWithMap(shopMap, new Shop(), false);
        }

        // 4、实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean stage = tryLock(lockKey);
            // 4.2 判断是否获取成功
            if (!stage) {
                // 4.3 失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.4 成功，根据id查询数据库
            shop = getById(id);

            Thread.sleep(200);  // 模拟在高并发访问数据库导致缓存重建的延时

            // 5、数据库中不存在数据则返回错误（请求无法查找到数据）
            if (shop == null) {
                // 解决缓存穿透问题，当数据库中不存在该数据时将空值写入redis缓存
                HashMap<Object, Object> blankMap = new HashMap<>();
                blankMap.put("", "");

                stringRedisTemplate.opsForHash().putAll(key, blankMap);
                stringRedisTemplate.expire(key, CACHE_NULL_TTL, TimeUnit.MINUTES);

                return null;
            }

            // 6、数据库存在，则先将数据写入redis缓存
            Map<String, Object> shopMapToCache = BeanUtil.beanToMap(shop, new HashMap<>(), CopyOptions.create()
                    .setIgnoreNullValue(true)  // 实际上不起作用，所以我在后面处理了fieldName为空的情况
                    .setFieldValueEditor((fieldName, fieldValue) -> fieldValue == null ? null : fieldValue.toString()));
            stringRedisTemplate.opsForHash().putAll(key, shopMapToCache);
            stringRedisTemplate.expire(key, CACHE_SHOP_TTL, TimeUnit.MINUTES);  // 追加设置超时时间
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7、释放互斥锁
            unlock(lockKey);
        }

        // 8、返回
        return shop;
    }

    // 创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 利用逻辑过期时间解决缓存击穿
     *
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id) {
        // 1、从redis中查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2、判断缓存是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 3、如果不存在则返回空
            return null;
        }

        // 4、缓存命中，先将json序列化成对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();  // 逻辑过期时间

        // 5、判断逻辑时间是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期，直接返回店铺信息
            return shop;
        }
        // 5.2 已过期，需要缓存重建

        // 6、缓存重建
        // 6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock) {
            // 6.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                // 重建缓存
                try {
                    this.saveShop2Redis(id, 20L);  // 设置20s为了方便测试，实际应设置30min
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }

        // 6.4 返回过期的商铺信息（获取锁成功，在本线程返回过期数据 or 获取锁失败）
        return shop;
    }

    // 尝试获取互斥锁
    private boolean tryLock(String key) {
        Boolean stage = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return stage != null && stage;
    }

    // 释放互斥锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    // 向redis缓存添加数据逻辑过期时间
    public void saveShop2Redis(Long id, Long expireTime) throws InterruptedException {
        // 1、查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);  // 模拟设置缓存重建时延
        // 2、封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        // 3、写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 更新数据库商铺信息 并删除缓存
     *
     * @param shop
     * @return
     */
    @Transactional  // 在service方法上添加事务，抛出异常时自动回滚
    @Override
    public Result update(Shop shop) {
        // 1、更新数据库
        Long id = shop.getId();
        if (id == null) {
            return Result.fail(String.format("id为%s的商铺不存在!", id));
        }
        updateById(shop);

        // 2、删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id.toString());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按照数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询Redis、按照距离排序、分页，结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
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
        // 4.1 截取 from ~ end 的部分
        ArrayList<Long> ids = new ArrayList<>(list.size());
        HashMap<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
