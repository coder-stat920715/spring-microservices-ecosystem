package com.architecture.orderquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
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
public class SecurityConfig {

    @Bean
    SecurityFilterChain api(HttpSecurity http) throws Exception {
        JwtAuthenticationConverter conv = new JwtAuthenticationConverter();
        conv.setJwtGrantedAuthoritiesConverter(SecurityConfig::authorities);
        return http
                .csrf(c -> c.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a -> a
                        .requestMatchers("/actuator/**", "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(conv)))
                .build();
    }

    private static Collection<org.springframework.security.core.GrantedAuthority> authorities(Jwt jwt) {
        Set<org.springframework.security.core.GrantedAuthority> out = new HashSet<>();
        Map<String, Object> realm = jwt.getClaim("realm_access");
        if (realm != null && realm.get("roles") instanceof Collection<?> rs) {
            rs.forEach(r -> out.add(new SimpleGrantedAuthority("ROLE_" + r.toString().toUpperCase())));
        }
        Collection<String> groups = jwt.getClaimAsStringList("cognito:groups");
        if (groups != null) groups.forEach(g -> out.add(new SimpleGrantedAuthority("ROLE_" + g.toUpperCase())));
        return out;
    }
}
