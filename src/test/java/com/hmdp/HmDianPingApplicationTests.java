package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private CacheClient client;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es= Executors.newFixedThreadPool(500);


    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        client.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+1L,shop,30L, TimeUnit.MINUTES);

    }

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task=()->{
            for (int i = 0; i < 100; i++) {
                Long id = redisIdWorker.nextId("order");
                System.out.println("id = "+id);
            }
            latch.countDown();
        };
        long begin=System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();

        long end=System.currentTimeMillis();
        System.out.println("time："+(end-begin));
    }

    @Test
    void testConcurrentUnique()throws InterruptedException{
        int threadCount = 50;
        int perThread = 2000;
        int total = threadCount*perThread;

        Set<Long> ids = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        Long begin = System.currentTimeMillis();
        for(int t = 0;t < threadCount;t++){
            pool.submit(()->{
                try{
                    for (int i = 0; i < threadCount; i++) {
                        ids.add(redisIdWorker.nextId("keyPrefix"));
                    }
               }finally {
                    latch.countDown();
                }
            });
            latch.await();
            long cost = System.currentTimeMillis() - begin;
            pool.shutdown();

            assertEquals(total, ids.size(), "并发下出现重复 ID！");
            System.out.printf(" 并发唯一性：%d 线程 × %d 个 = %d 个，耗时 %d ms，QPS ≈ %d%n",
                    threadCount, perThread, total, cost, total * 1000L / Math.max(cost, 1));
        }


    }
    @Test
    void testConcurrentUnique2() throws InterruptedException {
        int threadCount = 50;
        int perThread = 2000;
        int total = threadCount * perThread;

        Set<Long> ids = ConcurrentHashMap.newKeySet();
        Set<Long> duplicates = ConcurrentHashMap.newKeySet();       // ← 记录重复

        CountDownLatch startGun = new CountDownLatch(1);            // ← 发令枪
        CountDownLatch finishLine = new CountDownLatch(threadCount); // ← 终点线
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        long begin = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                try {
                    startGun.await();                    // 等枪响
                    for (int i = 0; i < perThread; i++) {
                        long id = redisIdWorker.nextId("prefix");
                        if (!ids.add(id)) {              // 捕获重复
                            duplicates.add(id);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLine.countDown();
                }
            });
        }

        startGun.countDown();            // 枪响
        finishLine.await();              // 等全部跑完

        long cost = System.currentTimeMillis() - begin;
        pool.shutdown();

        // 先报告重复情况（便于排查）
        if (!duplicates.isEmpty()) {
            System.out.println(" 发现 " + duplicates.size() + " 个重复 ID：" + duplicates);
        }

        assertEquals(total, ids.size(), "并发下出现重复 ID：" + duplicates);
        System.out.printf(" %d 线程 × %d 个 = %d 个 ID，无重复，耗时 %d ms，QPS ≈ %d%n",
                threadCount, perThread, total, cost, total * 1000L / Math.max(cost, 1));
    }

    //导入redisgeo店铺数据
    @Test
    void loadShopDate(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();

        //2.把店铺分组，按照typeId分组，id一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3.分批完成写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //获取类型id
            Long typeId = entry.getKey();
            //获取同类型店铺集合
            List<Shop>  value = entry.getValue();

            String key=SHOP_GEO_KEY+typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations=new ArrayList<>();
            for (Shop shop : value) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())
                ));
            }
            //写入redis
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

}
