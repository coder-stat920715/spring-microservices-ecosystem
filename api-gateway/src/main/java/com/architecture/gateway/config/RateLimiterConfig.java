package com.architecture.gateway.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Rate limiting is the Spring Cloud Gateway RequestRateLimiter filter, which is a Redis-backed
 * token bucket (see routes in application.yml: replenishRate / burstCapacity). The key resolver
 * buckets by authenticated user, falling back to client IP. Resilience4j supplies the per-route
 * circuit breaker below.
 */
@Configuration
public class RateLimiterConfig {

    @Bean
    KeyResolver userKeyResolver() {
        return exchange -> ReactiveSecurityContextHolder.getContext()
                .flatMap(ctx -> Mono.justOrEmpty(ctx.getAuthentication()))
                .map(Authentication::getName)
                .switchIfEmpty(Mono.fromSupplier(() -> clientIp(
                        exchange.getRequest().getHeaders().getFirst("X-Forwarded-For"),
                        exchange.getRequest().getRemoteAddress() == null ? null
                                : exchange.getRequest().getRemoteAddress().getAddress().getHostAddress())));
    }

    static String clientIp(String forwardedFor, String remote) {
        if (forwardedFor != null && !forwardedFor.isBlank()) {
            return forwardedFor.split(",")[0].trim();
        }
        return remote != null ? remote : "unknown";
    }

    @Bean
    Customizer<ReactiveResilience4JCircuitBreakerFactory> defaultCircuitBreakers() {
        return factory -> factory.configureDefault(id -> new Resilience4JConfigBuilder(id)
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .slidingWindowSize(20)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(15))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom().timeoutDuration(Duration.ofSeconds(5)).build())
                .build());
    }
}
