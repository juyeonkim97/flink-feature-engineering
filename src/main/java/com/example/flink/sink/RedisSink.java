package com.example.flink.sink;

import com.example.flink.model.SessionFeature;
import com.example.flink.model.UserFeature;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

public class RedisSink<T> extends RichSinkFunction<T> {
    private transient JedisPool jedisPool;
    private final String keyPrefix;
    private final int ttl;

    public RedisSink(String keyPrefix, int ttl) {
        this.keyPrefix = keyPrefix;
        this.ttl = ttl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        jedisPool = new JedisPool(config, "localhost", 6379);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = keyPrefix + ":" + getKey(value);
            jedis.hset(key, toMap(value));
            jedis.expire(key, ttl);
        }
    }

    private String getKey(T value) {
        if (value instanceof UserFeature) {
            return ((UserFeature) value).user_id;
        } else if (value instanceof SessionFeature) {
            SessionFeature sf = (SessionFeature) value;
            return sf.user_id + "_" + sf.timestamp;
        }
        return "unknown";
    }

    private Map<String, String> toMap(T value) {
        Map<String, String> map = new HashMap<>();
        if (value instanceof UserFeature) {
            UserFeature f = (UserFeature) value;
            map.put("user_id", f.user_id);
            map.put("view_count_1h", String.valueOf(f.view_count));
            map.put("cart_count_1h", String.valueOf(f.cart_count));
            map.put("purchase_count_1h", String.valueOf(f.purchase_count));
            map.put("avg_viewed_price_1h", String.valueOf(f.avg_viewed_price));
        } else if (value instanceof SessionFeature) {
            SessionFeature f = (SessionFeature) value;
            map.put("user_id", f.user_id);
            map.put("event_count", String.valueOf(f.event_count));
            map.put("duration_seconds", String.valueOf(f.duration_seconds));
        }
        return map;
    }

    @Override
    public void close() throws Exception {
        if (jedisPool != null) jedisPool.close();
    }
} 