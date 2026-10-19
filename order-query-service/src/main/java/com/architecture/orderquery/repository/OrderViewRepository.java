package com.architecture.orderquery.repository;

import com.architecture.orderquery.model.OrderView;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OrderViewRepository extends MongoRepository<OrderView, String> {
    List<OrderView> findByTenantId(String tenantId, Pageable pageable);
}
