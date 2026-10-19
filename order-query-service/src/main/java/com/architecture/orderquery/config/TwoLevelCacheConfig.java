package com.architecture.orderquery.config;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** L1 Caffeine (TTL 10s, max 500) + L2 Redis (TTL 10m), wired as the application CacheManager. */
@Configuration
@EnableCaching
public class TwoLevelCacheConfig {

    @Bean
    TwoLevelCacheManager cacheManager(RedisConnectionFactory cf, StringRedisTemplate redis) {
        RedisCacheConfiguration l2cfg = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(10))
                .disableCachingNullValues()
                .prefixCacheNameWith("oq:")
                .serializeValuesWith(SerializationPair.fromSerializer(
                        new JdkSerializationRedisSerializer(getClass().getClassLoader())));
        RedisCacheManager l2 = RedisCacheManager.builder(cf).cacheDefaults(l2cfg).build();
        l2.afterPropertiesSet();
        return new TwoLevelCacheManager(l2, redis, Duration.ofSeconds(10), 500);
    }

    @Bean
    RedisMessageListenerContainer l1EvictionListener(RedisConnectionFactory cf, TwoLevelCacheManager manager) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(cf);
        container.addMessageListener((message, pattern) ->
                manager.onRemoteEviction(new String(message.getBody(), StandardCharsets.UTF_8)),
                new ChannelTopic(TwoLevelCacheManager.CHANNEL));
        return container;
    }
}
