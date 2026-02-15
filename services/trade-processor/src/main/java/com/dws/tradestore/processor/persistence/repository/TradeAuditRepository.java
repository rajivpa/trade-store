package com.dws.tradestore.processor.persistence.repository;

import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import org.springframework.data.cassandra.repository.CassandraRepository;

import java.util.List;

public interface TradeAuditRepository extends CassandraRepository<TradeAuditEntity, String> {

    /**
     * Full history of Trade time ordered
     * @param tradeId
     * @return
     */
    List<TradeAuditEntity> findByTradeIdOrderByEventTimestampDesc(String tradeId);
}
