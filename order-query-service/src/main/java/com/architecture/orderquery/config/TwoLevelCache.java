package com.architecture.orderquery.config;

import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCache;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * L1 (Caffeine, in-process, 10s, 500 entries) in front of L2 (Redis, 10 min).
 * Reads: L1 -> L2 (promote to L1) -> miss. Writes go to both. Evictions hit both levels and are
 * broadcast over Redis pub/sub so other pods drop their L1 copy immediately.
 */
public class TwoLevelCache implements Cache {

    private final String name;
    private final CaffeineCache l1;
    private final Cache l2;
    private final BiConsumer<String, String> evictionBroadcaster;

    public TwoLevelCache(String name, CaffeineCache l1, Cache l2, BiConsumer<String, String> evictionBroadcaster) {
        this.name = name;
        this.l1 = l1;
        this.l2 = l2;
        this.evictionBroadcaster = evictionBroadcaster;
    }

    private static String k(Object key) {
        return String.valueOf(key);
    }

    @Override public String getName() { return name; }
    @Override public Object getNativeCache() { return l2.getNativeCache(); }

    @Override
    public ValueWrapper get(Object key) {
        String ck = k(key);
        ValueWrapper hit = l1.get(ck);
        if (hit != null) return hit;
        ValueWrapper remote = l2.get(ck);
        if (remote != null && remote.get() != null) l1.put(ck, remote.get());
        return remote;
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        ValueWrapper w = get(key);
        return w == null ? null : type.cast(w.get());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> loader) {
        ValueWrapper w = get(key);
        if (w != null) return (T) w.get();
        T value;
        try {
            value = loader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, loader, e);
        }
        put(key, value);
        return value;
    }

    @Override
    public void put(Object key, Object value) {
        l2.put(k(key), value);
        l1.put(k(key), value);
    }

    @Override
    public void evict(Object key) {
        l2.evict(k(key));
        l1.evict(k(key));
        evictionBroadcaster.accept(name, k(key));
    }

    @Override
    public void clear() {
        l2.clear();
        l1.clear();
        evictionBroadcaster.accept(name, "*");
    }

    /** Called when another pod announces an eviction. L2 is shared, so only L1 is touched. */
    void evictLocal(String key) {
        if ("*".equals(key)) l1.clear();
        else l1.evict(key);
    }
}
