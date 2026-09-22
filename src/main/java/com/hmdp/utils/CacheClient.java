package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import jodd.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;


@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;


    public CacheClient(StringRedisTemplate stringRedisTemplate, RedissonClient redissonClient) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
    }


    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(json)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.存在，返回商铺信息
            return JSONUtil.toBean(json, type);

        }
        //判断是否为空值
        if(json!=null){
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.判断数据库中是否存在
        if(r==null){
            //6.不存在，返回错误状态码
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //7.存在，写入redis，返回商铺信息
        this.set(key,r,time,unit);
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR= new ThreadPoolExecutor(4,
            8,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(200),
            new ThreadFactoryBuilder()
                    .setNameFormat("cache-rebuild-%d")
                    .get(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isBlank(json)){
            return null;
        }

        //4.存在，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R shop = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1.未过期，直接返回店铺信息
            return shop;
        }
        //6.1.获取互斥锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        boolean islock = tryLock(lockKey);
        //6.2.判断是否获取锁成功
        if(islock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,r,time,unit);
                } catch (Exception e) {
                    log.error(e.getMessage(),e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        return shop;
    }//  6.3.成功，开启独立线程实现缓存重建

                   //查询数据库

                    //写入redis

                    //释放锁：必须释放 lockKey，而非缓存键 key


        //6.4.返回过期的商铺信息


    private final RedissonClient redissonClient;

    public <R,ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                   Long time, TimeUnit unit) throws InterruptedException{
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json,type);
        }
        if(json != null){
            return null;
        }
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        RLock lock = redissonClient.getLock(lockKey);
        boolean islock = false;

        final int MAX_RETRY = 5;
        int retry = 0;
        try{
            while(retry < MAX_RETRY){
                islock = lock.tryLock(1, TimeUnit.SECONDS);
                if(islock){
                    break;
                }
                retry++;
                log.warn("抢锁失败，准备重试");
            }
            if(!islock){
                return dbFallback.apply(id);
            }
            json = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(json)){
                return JSONUtil.toBean(json,type);
            }
            R r = dbFallback.apply(id);
            if(r == null){
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            this.set(key,r,time,unit);
            return r;
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }finally {
            if(islock && lock.isHeldByCurrentThread()){
                lock.unlock();
            }
        }
    }



    /**
     * 创建锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 封闭锁
     * @param key
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
