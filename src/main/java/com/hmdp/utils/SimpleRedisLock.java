package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));  // 默认的类加载器加载String路径下的资源
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取 JVM 线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);  // 自动拆箱时防止空指针的写法
    }

    @Override
    public void unLock() {
        // 调用lua脚本  -- 满足原子性
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(stringRedisTemplate.opsForValue().get(KEY_PREFIX + name)),
                ID_PREFIX + Thread.currentThread().getId()  // 线程标识
        );
    }

//    @Override
//    public void unLock() {
//        // 获取线程标识
//        String threadID = ID_PREFIX + Thread.currentThread().getId();
//        // 获取锁标识
//        String lockId = stringRedisTemplate.opsForValue().get(ID_PREFIX + name);
//        if (threadID.equals(lockId)) {
//            // 如果一致，才释放锁
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
}
