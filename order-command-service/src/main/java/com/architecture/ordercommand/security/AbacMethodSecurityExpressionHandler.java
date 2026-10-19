package com.architecture.ordercommand.security;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.security.access.expression.method.DefaultMethodSecurityExpressionHandler;
import org.springframework.security.access.expression.method.MethodSecurityExpressionOperations;
import org.springframework.security.core.Authentication;

import java.util.function.Supplier;

/** Swaps the SpEL root object for one that also understands the ABAC predicates. */
public class AbacMethodSecurityExpressionHandler extends DefaultMethodSecurityExpressionHandler {

    private final AbacProperties props;

    public AbacMethodSecurityExpressionHandler(AbacProperties props) {
        this.props = props;
    }

    @Override
    public EvaluationContext createEvaluationContext(Supplier<Authentication> authentication, MethodInvocation mi) {
        StandardEvaluationContext context = (StandardEvaluationContext) super.createEvaluationContext(authentication, mi);
        MethodSecurityExpressionOperations delegate =
                (MethodSecurityExpressionOperations) context.getRootObject().getValue();
        AbacExpressionRoot root = new AbacExpressionRoot(delegate.getAuthentication(), props);
        root.setThis(delegate.getThis());
        context.setRootObject(root);
        return context;
    }
}
