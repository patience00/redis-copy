package com.linchtech.redis_sync.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.ConvertingCursor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

/**
 * @author: 107
 * @date: 2020-01-05 18:24
 * @description:
 **/
@Component
@Slf4j
public class Sync implements ApplicationRunner {

    @Autowired
    @Qualifier("redisTemplate")
    private RedisTemplate firstRedisTemplate;

    @Autowired
    @Qualifier("redisTemplate2")
    private RedisTemplate secondRedisTemplate;


    @Override
    public void run(ApplicationArguments args) throws Exception {
        ScanOptions options =
                ScanOptions.scanOptions().match("lafarge-order-calc:").count(Integer.MAX_VALUE).build();
        RedisSerializer<String> redisSerializer = (RedisSerializer<String>) firstRedisTemplate.getKeySerializer();
        Cursor<String> cursor = (Cursor) firstRedisTemplate.executeWithStickyConnection(redisConnection ->
                new ConvertingCursor<>(redisConnection.scan(options), redisSerializer::deserialize));
        int keySize = 0;
        while (cursor.hasNext()) {
            String key = cursor.next();
            String value = (String) firstRedisTemplate.opsForValue().get(key);
            keySize++;
            log.info("key:{},value:{}", key, value);
            secondRedisTemplate.opsForValue().set(key, value);
        }
        log.info("keys:{}个", keySize);
    }
}
