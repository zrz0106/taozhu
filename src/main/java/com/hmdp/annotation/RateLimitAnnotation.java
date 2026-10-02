package com.hmdp.annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


public class RateLimitAnnotation {
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface RateLimit {
        int capacity() default 50;    // 桶容量，决定能容忍多大的突发
        int rate() default 50;        // 每秒补充的令牌数，决定长期速率
        LimitType type() default LimitType.GLOBAL;
        String prefix() default "rate";
        String message() default "当前参与人数过多，请稍后再试";
    }

    public enum LimitType { GLOBAL, USER }
}
