package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.MeterReading;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;

import java.util.ArrayList;
import java.util.List;

public class FeedDaoRedisImpl implements FeedDao {

    private static final long globalMaxFeedLength = 10000;
    private static final long siteMaxFeedLength = 2440;
    private final JedisPool jedisPool;

    public FeedDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // Challenge #6
    @Override
    public void insert(MeterReading meterReading) {
        final String globalFeedKey = RedisSchema.getGlobalFeedKey();
        final String siteFeedKey = RedisSchema.getFeedKey(meterReading.getSiteId());

        try (Jedis jedis = jedisPool.getResource()) {

            jedis.xadd(globalFeedKey, null, meterReading.toMap(), globalMaxFeedLength, true);
            jedis.xadd(siteFeedKey, null, meterReading.toMap(), siteMaxFeedLength, true);
        }
    }

    @Override
    public List<MeterReading> getRecentGlobal(int limit) {
        return getRecent(RedisSchema.getGlobalFeedKey(), limit);
    }

    @Override
    public List<MeterReading> getRecentForSite(long siteId, int limit) {
        return getRecent(RedisSchema.getFeedKey(siteId), limit);
    }

    public List<MeterReading> getRecent(String key, int limit) {
        List<MeterReading> readings = new ArrayList<>(limit);
        try (Jedis jedis = jedisPool.getResource()) {
            List<StreamEntry> entries = jedis.xrevrange(key, null, null, limit);
            for (StreamEntry entry : entries) {
                readings.add(new MeterReading(entry.getFields()));
            }
            return readings;
        }
    }
}
