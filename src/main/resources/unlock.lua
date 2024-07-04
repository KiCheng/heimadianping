-- redis锁中的标识
local lockId = redis.call('get', KEYS[1])

-- ARGV[1] 是线程标识

-- 比较线程标识与锁中的标识是否一致
if (lockId == ARGV[1]) then
    -- 释放锁
    return redis.call('del', KEYS[1])
end
return 0