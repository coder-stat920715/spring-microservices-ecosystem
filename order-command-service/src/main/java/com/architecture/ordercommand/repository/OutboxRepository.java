package com.architecture.ordercommand.repository;

import com.architecture.ordercommand.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    @Modifying
    @Query("delete from OutboxEvent e where e.createdAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") Instant cutoff);
}
