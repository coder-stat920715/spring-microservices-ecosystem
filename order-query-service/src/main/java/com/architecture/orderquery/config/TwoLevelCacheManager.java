package com.architecture.orderquery.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TwoLevelCacheManager implements CacheManager {

    static final String CHANNEL = "cache:l1-evict";
    private final String instanceId = UUID.randomUUID().toString();
    private final CacheManager l2;
    private final StringRedisTemplate redis;
    private final Duration l1Ttl;
    private final long l1MaxSize;
    private final ConcurrentMap<String, TwoLevelCache> caches = new ConcurrentHashMap<>();

    public TwoLevelCacheManager(CacheManager l2, StringRedisTemplate redis, Duration l1Ttl, long l1MaxSize) {
        this.l2 = l2;
        this.redis = redis;
        this.l1Ttl = l1Ttl;
        this.l1MaxSize = l1MaxSize;
    }

    @Override
    public Cache getCache(String name) {
        return caches.computeIfAbsent(name, n -> {
            Cache remote = l2.getCache(n);
            if (remote == null) throw new IllegalStateException("L2 cache unavailable: " + n);
            CaffeineCache local = new CaffeineCache(n, Caffeine.newBuilder()
                    .expireAfterWrite(l1Ttl).maximumSize(l1MaxSize).recordStats().build(), false);
            return new TwoLevelCache(n, local, remote, this::broadcast);
        });
    }

    @Override
    public Collection<String> getCacheNames() {
        return caches.keySet();
    }

    private void broadcast(String cache, String key) {
        redis.convertAndSend(CHANNEL, instanceId + "|" + cache + "|" + key);
    }

    /** Message format: instanceId|cacheName|key */
    void onRemoteEviction(String message) {
        String[] p = message.split("\\|", 3);
        if (p.length < 3 || instanceId.equals(p[0])) return;
        TwoLevelCache c = caches.get(p[1]);
        if (c != null) c.evictLocal(p[2]);
    }
}
