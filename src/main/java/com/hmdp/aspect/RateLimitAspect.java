package com.hmdp.aspect;

import com.hmdp.annotation.RateLimitAnnotation;
import com.hmdp.exception.RateLimitException;
import com.hmdp.utils.RateLimiter;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    @Resource
    private RateLimiter rateLimiter;

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimitAnnotation.RateLimit rateLimit) throws Throwable {
        String key = buildKey(joinPoint, rateLimit);
        boolean allowed = rateLimiter
                .tryAcquire(key, rateLimit.capacity(), rateLimit.rate());

        if (!allowed) {
            log.warn("触发限流，key={}", key);
            throw new RateLimitException(rateLimit.message());
        }
        return joinPoint.proceed();
    }

    private String buildKey(ProceedingJoinPoint joinPoint, RateLimitAnnotation.RateLimit rateLimit) {
        if (rateLimit.type() == RateLimitAnnotation.LimitType.USER) {
            Long userId = UserHolder.getUser().getId();
            return rateLimit.prefix() + ":user:" + userId;
        }
        // 全局维度：用方法名 + 第一个参数区分不同活动
        Object[] args = joinPoint.getArgs();
        return rateLimit.prefix() + ":global:" + args[0];
    }
}

