package com.architecture.orderquery.service;

import com.architecture.orderquery.model.OrderView;
import com.architecture.orderquery.repository.OrderViewRepository;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.NoSuchElementException;

@Service
public class OrderQueryService {

    private final OrderViewRepository repo;

    public OrderQueryService(OrderViewRepository repo) {
        this.repo = repo;
    }

    @Cacheable(cacheNames = "orders", key = "#id")
    public OrderView byId(String id) {
        return repo.findById(id).orElseThrow(() -> new NoSuchElementException("order " + id));
    }

    public List<OrderView> byTenant(String tenantId, int page, int size) {
        return repo.findByTenantId(tenantId, PageRequest.of(page, Math.min(size, 100)));
    }
}
