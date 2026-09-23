package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    //开始时间戳
    private static final long BEGIN_TIMESTAMP=1640995200L;

    //序列号位数
    private static final int COUNT_BITS=32;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
public Long nextId(String keyPrefix) {
    //生成时间戳
    LocalDateTime now = LocalDateTime.now();
    long nowsecond = now.toEpochSecond(ZoneOffset.UTC);
    long timestamp = nowsecond - BEGIN_TIMESTAMP;

    //生成序列号
    String data = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
    Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + data);
    return timestamp << COUNT_BITS | count;
    }
}
