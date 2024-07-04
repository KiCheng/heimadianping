package com.hmdp.utils;


import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        // 设置逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存穿透
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit timeUnit) {
        // 1、从redis中查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2、判断缓存是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3、缓存命中，直接返回
            return JSONUtil.toBean(json, type);
        }

        // 解决缓存穿透--判断命中的是否为空值
        if (json != null) {
            // 命中的为空值，返回一个错误信息
            return null;
        }

        // 4、若不存在则根据id查询数据库
        R r = dbFallback.apply(id);

        // 5、数据库中不存在数据则返回错误（请求无法查找到数据）
        if (r == null) {
            // 解决缓存穿透问题，当数据库中不存在该数据时将空值写入redis缓存
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }

        // 6、数据库存在，则先将数据写入redis缓存
        this.set(key, r, time, timeUnit);

        return r;
    }


    // 创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public <R, ID>R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, String lockPrefix, Long time, TimeUnit timeUnit) {
        // 1、从redis中查询缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2、判断缓存是否存在
        if (StrUtil.isBlank(json)) {
            // 3、如果不存在则返回空
            return null;
        }

        // 4、缓存命中，先将json序列化成对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
//        Object dataObj = redisData.getData();
//        JSONObject data = null;
//        if (dataObj instanceof String) {
//            String jsonData = (String) dataObj;
//            data = JSONUtil.parseObj(jsonData);
//        }
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5、判断逻辑时间是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期，直接返回
            return r;
        }
        // 5.2 已过期，需要缓存重建

        // 6、缓存重建
        // 6.1 获取互斥锁
        String lockKey = lockPrefix + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock) {
            // 6.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                // 重建缓存
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 封装逻辑过期时间并写入Redis
                    this.setWithLogicalExpire(key, r1, time, timeUnit);
                    
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }

        // 6.4 返回过期的信息（获取锁成功，在本线程返回过期数据 or 获取锁失败）
        return r;
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
}
