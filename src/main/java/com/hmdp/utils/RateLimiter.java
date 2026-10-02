package com.hmdp.utils;


import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import javax.annotation.Resource;
import java.util.Collections;

public class RateLimiter {
    @Resource
    StringRedisTemplate stringRedisTemplate;
    private static final DefaultRedisScript<Long> TOKEN_BUCKET_SCRIPT;
    static {
        TOKEN_BUCKET_SCRIPT = new DefaultRedisScript<>();
        TOKEN_BUCKET_SCRIPT.setLocation(new ClassPathResource("rate_limit.lua"));
        TOKEN_BUCKET_SCRIPT.setResultType(Long.class);
    }

    public boolean tryAcquire(String key, int capacity, int rate) {
        Long result = stringRedisTemplate.execute(
                TOKEN_BUCKET_SCRIPT,
                Collections.singletonList(key),      // KEYS
                String.valueOf(capacity),
                String.valueOf(rate),
                String.valueOf(System.currentTimeMillis()),
                "1"   //本次消耗几个令牌
        );
        return result != null && result == 1L;
    }
}
