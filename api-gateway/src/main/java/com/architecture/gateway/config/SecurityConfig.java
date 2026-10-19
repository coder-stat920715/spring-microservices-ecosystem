package com.architecture.gateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Zero-trust edge: every request except health/docs must carry a valid OIDC JWT
 * (Keycloak or AWS Cognito). JWKS keys are cached and re-fetched automatically when an
 * unknown key id (kid) shows up, which gives transparent key rotation.
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(ex -> ex
                        .pathMatchers("/health", "/actuator/**", "/fallback/**",
                                "/swagger-ui.html", "/swagger-ui/**", "/v3/api-docs/**",
                                "/webjars/**", "/docs/**").permitAll()
                        .anyExchange().authenticated())
                .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(reactiveJwtConverter())))
                .build();
    }

    Converter<Jwt, Mono<AbstractAuthenticationToken>> reactiveJwtConverter() {
        JwtAuthenticationConverter delegate = new JwtAuthenticationConverter();
        delegate.setJwtGrantedAuthoritiesConverter(SecurityConfig::extractAuthorities);
        return new ReactiveJwtAuthenticationConverterAdapter(delegate);
    }

    /** Maps Keycloak realm roles and Cognito groups to ROLE_* authorities. */
    static Collection<GrantedAuthority> extractAuthorities(Jwt jwt) {
        Set<GrantedAuthority> out = new HashSet<>();
        Map<String, Object> realm = jwt.getClaim("realm_access");
        if (realm != null && realm.get("roles") instanceof Collection<?> roles) {
            roles.forEach(r -> out.add(new SimpleGrantedAuthority("ROLE_" + r.toString().toUpperCase())));
        }
        Collection<String> groups = jwt.getClaimAsStringList("cognito:groups");
        if (groups != null) {
            groups.forEach(g -> out.add(new SimpleGrantedAuthority("ROLE_" + g.toUpperCase())));
        }
        return out;
    }
}
