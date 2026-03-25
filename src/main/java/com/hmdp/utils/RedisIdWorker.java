package com.hmdp.utils;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
@RequiredArgsConstructor
public class RedisIdWorker {
    private static final Long BEGIN_TIMESTAMP = 1605052800L;
    private static final Integer COUNT_BITS = 32;
    private final StringRedisTemplate stringRedisTemplate;
    public Long nextId(String keyPrefix){
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        Long timestamp=nowSecond-BEGIN_TIMESTAMP;
        // 2.生成序列号
        String date=now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.生成序列号
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // 3.拼接并返回
        return timestamp<<COUNT_BITS|count;
    }

    public static void main(String[] args) {
        long second = LocalDateTime.of(2020, 11, 11, 0, 0, 0).toEpochSecond(ZoneOffset.UTC);
        System.out.println("second="+second);
    }
}
