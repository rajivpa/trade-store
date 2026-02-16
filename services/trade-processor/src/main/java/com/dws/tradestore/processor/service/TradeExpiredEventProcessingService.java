package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TradeExpiredEventProcessingService {

    @Autowired
    private TradeWriteService tradeWriteService;

    /**
     * Triggered from expiration events (sent after updating SQL store). On receipt
     * of an expiration event, update the audit store and state store.
     * @param tradeExpiredEvent expired trade event payload
     */
    public void processExpiredEventForAuditAndStateStore(TradeExpiredEvent tradeExpiredEvent) {
        try {
            tradeWriteService.expireTradeInNoSqlAndStateStore(tradeExpiredEvent);
        } catch (Exception e) {
            log.error(
                    "Error during expiring trade in No SQL store/ State store for tradeId: {}",
                    tradeExpiredEvent.getTradeId(),
                    e
            );
            throw new RuntimeException("Error during saving of expired trade event", e);
        }
    }
}
