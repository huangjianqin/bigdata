package org.kin.redis.bloomfilter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * @author huangjianqin
 * @date 2022/3/4
 */
public class RedisBloomFilterTest {
    public static void main(String[] args) {
        RedisURI redisUri = RedisURI.builder()
                .withHost("0.0.0.0")
                .withPort(6379)
                .withTimeout(Duration.of(10, ChronoUnit.SECONDS))
                .build();
        RedisClient redisClient = RedisClient.create(redisUri);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> commands = connection.sync();
        String key = "bf";
        commands.del(key);

        RedisBloomFilter<String> bloomFilter = new RedisBloomFilter<>(key, 1000, 0.03, connection);

        bloomFilter.put("1");
        bloomFilter.put("2");
        bloomFilter.put("3");
        bloomFilter.put("4");
        bloomFilter.put("5");
        bloomFilter.put("6");
        bloomFilter.put("7");

        System.out.println(bloomFilter.contains("1"));
        System.out.println(bloomFilter.contains("2"));
        System.out.println(bloomFilter.contains("3"));
        System.out.println(bloomFilter.contains("4"));
        System.out.println(bloomFilter.contains("5"));
        System.out.println(bloomFilter.contains("6"));
        System.out.println(bloomFilter.contains("7"));
        System.out.println("---------------------------");
        System.out.println(bloomFilter.contains("8"));
        System.out.println(bloomFilter.contains("9"));
        System.out.println(bloomFilter.contains("10"));
        System.out.println(bloomFilter.contains("aa"));
        System.out.println(bloomFilter.contains("bbfdsfsdfsdfsdfs"));
        System.out.println(bloomFilter.contains("cc"));
        System.out.println(bloomFilter.contains("dd"));
        System.out.println(bloomFilter.contains("ee"));

        connection.close();
        redisClient.shutdown();
    }
}
