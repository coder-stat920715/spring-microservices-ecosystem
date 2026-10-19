package com.architecture.ordercommand.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/** Puts userId / tenantId / clientIp into the SLF4J MDC (traceId/spanId come from Micrometer Tracing). */
@Component
public class MdcContextFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        try {
            Authentication a = SecurityContextHolder.getContext().getAuthentication();
            if (a instanceof JwtAuthenticationToken t) {
                MDC.put("userId", String.valueOf(t.getToken().getSubject()));
                String tenant = t.getToken().getClaimAsString("tenant_id");
                if (tenant != null) MDC.put("tenantId", tenant);
            }
            String xff = req.getHeader("X-Forwarded-For");
            MDC.put("clientIp", xff != null && !xff.isBlank() ? xff.split(",")[0].trim() : req.getRemoteAddr());
            chain.doFilter(req, res);
        } finally {
            MDC.remove("userId");
            MDC.remove("tenantId");
            MDC.remove("clientIp");
        }
    }
}
