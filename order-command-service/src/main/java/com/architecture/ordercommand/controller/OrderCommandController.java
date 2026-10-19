package com.architecture.ordercommand.controller;

import com.architecture.ordercommand.concurrency.VirtualThreadOrderProcessor;
import com.architecture.ordercommand.domain.OrderEntity;
import com.architecture.ordercommand.dto.Dtos.*;
import com.architecture.ordercommand.service.OrderCommandService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderCommandController {

    private final OrderCommandService service;
    private final VirtualThreadOrderProcessor processor;

    public OrderCommandController(OrderCommandService service, VirtualThreadOrderProcessor processor) {
        this.service = service;
        this.processor = processor;
    }

    /** RBAC (role) combined with ABAC (tenant, risk score, time window, location). */
    @PostMapping
    @Operation(summary = "CREATE_ORDER command")
    @PreAuthorize("hasRole('ROLE_ADMIN') or (hasAnyRole('ROLE_CUSTOMER','ROLE_OPERATOR')"
            + " and tenantMatches(#cmd.tenantId()) and riskBelow(70) and inTimeWindow() and locationAllowed())")
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderCommand cmd) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(toResponse(service.create(cmd)));
    }

    @PutMapping("/{id}")
    @Operation(summary = "UPDATE_ORDER command")
    @PreAuthorize("hasRole('ROLE_ADMIN') or (hasRole('ROLE_OPERATOR') and riskBelow(50) and inTimeWindow())")
    public OrderResponse update(@PathVariable UUID id, @Valid @RequestBody UpdateOrderCommand cmd) {
        return toResponse(service.update(id, cmd));
    }

    @PostMapping("/batch")
    @Operation(summary = "Concurrent batch creation on virtual threads")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public BatchResult batch(@RequestBody List<@Valid CreateOrderCommand> commands) {
        return processor.processBatch(commands).join();
    }

    @GetMapping("/_stats")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Map<String, Long> stats(@RequestParam(defaultValue = "false") boolean reset) {
        return processor.snapshot(reset);
    }

    private static OrderResponse toResponse(OrderEntity o) {
        return new OrderResponse(o.getId(), o.getStatus().name(), o.getVersion());
    }
}
