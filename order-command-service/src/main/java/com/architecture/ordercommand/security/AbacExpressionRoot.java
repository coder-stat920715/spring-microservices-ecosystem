package com.architecture.ordercommand.security;

import jakarta.servlet.http.HttpServletRequest;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.security.access.expression.SecurityExpressionRoot;
import org.springframework.security.access.expression.method.MethodSecurityExpressionOperations;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Adds attribute-based predicates usable inside @PreAuthorize expressions:
 * tenantMatches(x), riskBelow(n), inTimeWindow(), locationAllowed().
 * Tenant comes from the verified JWT; risk score / location are set by the gateway
 * (which strips client-supplied copies) and reach us only over mTLS.
 */
public class AbacExpressionRoot extends SecurityExpressionRoot implements MethodSecurityExpressionOperations {

    private final AbacProperties props;
    private Object filterObject;
    private Object returnObject;
    private Object target;

    public AbacExpressionRoot(Authentication authentication, AbacProperties props) {
        super(authentication);
        this.props = props;
    }

    public boolean tenantMatches(String tenantId) {
        if (tenantId == null || !(getAuthentication().getPrincipal() instanceof Jwt jwt)) return false;
        return tenantId.equals(jwt.getClaimAsString("tenant_id"));
    }

    public boolean riskBelow(int max) {
        String raw = header("X-Request-Risk-Score");
        if (raw == null) return false;             // fail closed
        try {
            return Integer.parseInt(raw.trim()) < max;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean inTimeWindow() {
        LocalTime now = LocalTime.now(ZoneId.of(props.zone()));
        LocalTime s = props.windowStart();
        LocalTime e = props.windowEnd();
        return s.isBefore(e) ? !now.isBefore(s) && now.isBefore(e) : !now.isBefore(s) || now.isBefore(e);
    }

    public boolean locationAllowed() {
        String loc = header("X-User-Location");
        return loc != null && props.allowedLocations().contains(loc.toUpperCase());
    }

    private static String header(String name) {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes a) {
            HttpServletRequest r = a.getRequest();
            return r.getHeader(name);
        }
        return null;
    }

    @Override public void setFilterObject(Object o) { this.filterObject = o; }
    @Override public Object getFilterObject() { return filterObject; }
    @Override public void setReturnObject(Object o) { this.returnObject = o; }
    @Override public Object getReturnObject() { return returnObject; }
    @Override public Object getThis() { return target; }
    public void setThis(Object target) { this.target = target; }
}
