package com.architecture.ordercommand.security;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.access.expression.method.MethodSecurityExpressionHandler;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Configuration
@EnableMethodSecurity
@EnableConfigurationProperties(AbacProperties.class)
public class SecurityConfig {

    @Bean
    SecurityFilterChain api(HttpSecurity http) throws Exception {
        return http
                .csrf(c -> c.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a -> a
                        .requestMatchers("/actuator/**", "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(jwtConverter())))
                .build();
    }

    @Bean
    static MethodSecurityExpressionHandler methodSecurityExpressionHandler(AbacProperties props) {
        return new AbacMethodSecurityExpressionHandler(props);
    }

    static JwtAuthenticationConverter jwtConverter() {
        JwtAuthenticationConverter c = new JwtAuthenticationConverter();
        Converter<Jwt, Collection<GrantedAuthority>> roles = jwt -> {
            Set<GrantedAuthority> out = new HashSet<>();
            Map<String, Object> realm = jwt.getClaim("realm_access");
            if (realm != null && realm.get("roles") instanceof Collection<?> rs) {
                rs.forEach(r -> out.add(new SimpleGrantedAuthority("ROLE_" + r.toString().toUpperCase())));
            }
            Collection<String> groups = jwt.getClaimAsStringList("cognito:groups");
            if (groups != null) groups.forEach(g -> out.add(new SimpleGrantedAuthority("ROLE_" + g.toUpperCase())));
            return out;
        };
        c.setJwtGrantedAuthoritiesConverter(roles);
        return c;
    }
}
