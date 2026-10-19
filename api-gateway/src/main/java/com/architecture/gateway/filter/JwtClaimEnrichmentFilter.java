package com.architecture.gateway.filter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 1. Token revocation: rejects JWTs whose jti is in the Redis deny-list ("revoked:jti:&lt;id&gt;"),
 *    written by the IdP logout/event hook.
 * 2. Token relay: forwards the validated bearer token downstream (services re-validate it).
 * 3. Claim enrichment: strips any client-supplied trust headers, then sets X-User-Id, X-Tenant-Id,
 *    X-User-Roles, X-User-Location and X-Request-Risk-Score for ABAC at the services.
 */
@Component
public class JwtClaimEnrichmentFilter implements GlobalFilter, Ordered {

    private static final String REVOKED_PREFIX = "revoked:jti:";
    private final ReactiveStringRedisTemplate redis;
    private final boolean failOpen;

    public JwtClaimEnrichmentFilter(ReactiveStringRedisTemplate redis,
                                    @Value("${app.security.revocation.fail-open:false}") boolean failOpen) {
        this.redis = redis;
        this.failOpen = failOpen;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        Mono<Optional<JwtAuthenticationToken>> auth = ReactiveSecurityContextHolder.getContext()
                .flatMap(ctx -> Mono.justOrEmpty(ctx.getAuthentication()))
                .filter(JwtAuthenticationToken.class::isInstance)
                .map(a -> Optional.of((JwtAuthenticationToken) a))
                .defaultIfEmpty(Optional.empty());

        return auth.flatMap(opt -> {
            if (opt.isEmpty()) {
                return chain.filter(exchange);
            }
            JwtAuthenticationToken token = opt.get();
            Jwt jwt = token.getToken();
            Mono<Boolean> revoked = jwt.getId() == null ? Mono.just(false)
                    : redis.hasKey(REVOKED_PREFIX + jwt.getId())
                            .timeout(Duration.ofMillis(200))
                            .onErrorReturn(!failOpen);
            return revoked.flatMap(isRevoked -> {
                if (isRevoked) {
                    exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                    return exchange.getResponse().setComplete();
                }
                return chain.filter(exchange.mutate().request(enrich(exchange, token)).build());
            });
        });
    }

    private ServerHttpRequest enrich(ServerWebExchange exchange, JwtAuthenticationToken token) {
        Jwt jwt = token.getToken();
        String roles = token.getAuthorities().stream().map(GrantedAuthority::getAuthority)
                .collect(Collectors.joining(","));
        String country = exchange.getRequest().getHeaders().getFirst("CloudFront-Viewer-Country");
        return exchange.getRequest().mutate().headers(h -> {
            h.remove("X-User-Id");
            h.remove("X-Tenant-Id");
            h.remove("X-User-Roles");
            h.remove("X-User-Location");
            h.remove("X-Request-Risk-Score");
            h.set(HttpHeaders.AUTHORIZATION, "Bearer " + jwt.getTokenValue());
            if (jwt.getSubject() != null) h.set("X-User-Id", jwt.getSubject());
            String tenant = jwt.getClaimAsString("tenant_id");
            if (tenant != null) h.set("X-Tenant-Id", tenant);
            h.set("X-User-Roles", roles);
            if (country != null) h.set("X-User-Location", country);
            h.set("X-Request-Risk-Score", String.valueOf(riskScore(exchange, jwt)));
        }).build();
    }

    /**
     * Deliberately simple heuristic; plug a real risk engine in here. Baseline 10,
     * +30 for a missing User-Agent, +40 if the interactive login is older than one hour.
     */
    static int riskScore(ServerWebExchange exchange, Jwt jwt) {
        int score = 10;
        if (exchange.getRequest().getHeaders().getFirst(HttpHeaders.USER_AGENT) == null) score += 30;
        Instant authTime = jwt.getClaimAsInstant("auth_time");
        if (authTime != null && authTime.isBefore(Instant.now().minus(Duration.ofHours(1)))) score += 40;
        return score;
    }

    @Override
    public int getOrder() {
        return -1;
    }
}
