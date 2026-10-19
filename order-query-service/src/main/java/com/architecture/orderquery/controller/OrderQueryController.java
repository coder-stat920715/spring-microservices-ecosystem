package com.architecture.orderquery.controller;

import com.architecture.orderquery.model.OrderView;
import com.architecture.orderquery.service.OrderQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/api/orders")
public class OrderQueryController {

    private final OrderQueryService service;

    public OrderQueryController(OrderQueryService service) {
        this.service = service;
    }

    /** RBAC + ABAC: non-admins may only read orders of their own tenant (tenant_id JWT claim). */
    @GetMapping("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_ADMIN','ROLE_CUSTOMER','ROLE_OPERATOR')")
    @PostAuthorize("hasRole('ROLE_ADMIN') or returnObject.tenantId() == authentication.principal.getClaimAsString('tenant_id')")
    public OrderView get(@PathVariable String id) {
        return service.byId(id);
    }

    @GetMapping
    @PreAuthorize("hasRole('ROLE_ADMIN') or #tenantId == authentication.principal.getClaimAsString('tenant_id')")
    public List<OrderView> list(@RequestParam String tenantId,
                                @RequestParam(defaultValue = "0") int page,
                                @RequestParam(defaultValue = "20") int size) {
        return service.byTenant(tenantId, page, size);
    }

    @ExceptionHandler(NoSuchElementException.class)
    ProblemDetail notFound(NoSuchElementException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }
}
