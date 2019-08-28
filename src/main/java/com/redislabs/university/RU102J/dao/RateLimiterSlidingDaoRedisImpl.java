package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Random;

import static java.time.Instant.now;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS, long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {

        //[limiter]:[windowSize]:[name]:[maxHits]
        try (Jedis jedis = jedisPool.getResource()) {
            String key = "limiter:" + windowSizeMS + ":" + name + ":" + maxHits;


            Pipeline pipeline = jedis.pipelined();
            long timestamp = now().toEpochMilli();
            Response<Long> hits = pipeline.zadd(key, timestamp, String.valueOf(timestamp) + "-" + new Random().nextInt());

            long older = timestamp - windowSizeMS;

            Response<Long> removed = pipeline.zremrangeByScore(key, 0, older);

            Response<Long> elements = pipeline.zcard(key);

            pipeline.sync();
            if (elements.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }


    }
}
