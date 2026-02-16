package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import com.dws.tradestore.processor.persistence.repository.TradeAuditRepository;
import com.dws.tradestore.processor.persistence.repository.TradeEntityRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
public class TradeWriteService {

    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 5000;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final int MAX_RETRIES = 3;

    @Autowired
    private TradeEntityRepository tradeEntityRepository;

    @Autowired
    private TradeAuditRepository tradeAuditRepository;

    @Autowired
    private TradeStateStoreService tradeStateService;

    @Autowired
    private TradeMapper tradeMapper;

    public void acceptTrade(Trade trade) {

        log.info("Writing Trade with tradeId={}, version={}, to state store & persistent storages", trade.getTradeId(), trade.getVersion());
        // NOTE: Known limitation: writes span SQL, NoSQL and state-update Kafka publish.
        // This is not a single atomic transaction across systems, so partial-success scenarios are possible.
        // Future enhancement: outbox/event-driven reconciliation to improve cross-store consistency guarantees.

        try {
            TradeEntity tradeEntity = tradeMapper.domainToSQLEntity(trade);
            TradeAuditEntity tradeAuditEntity = tradeMapper.domainToNoSqlAuditEntity(trade);
            TradeState tradeState = tradeMapper.domainToStateEntity(trade);

            //1: Update SQL STORE
            log.debug("Updating SQL Store");
            updateSqlStore(tradeEntity);
            log.info("Trade SQL Store updated for : trade id={}, version={}",
                    tradeEntity.getTradeId(), tradeEntity.getVersion());

            //2. Update No SQL store for history/audit
            log.debug("Updating No SQL Store for History/Audit ");
            updateNoSqlStore(tradeAuditEntity);
            log.info("No SQL Store updated for : trade id={}, version={}",
                    tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion());

            //3: Update Trade State Store
            log.debug("updating Trade State Store...");
            publishUpdateEventForTradeStateStore(tradeState);
            log.info("Trade State Store updated for : trade id={}, version={}",
                    tradeState.getTradeId(), tradeState.getLastProcessedVersion());

            log.info("Trade write operation completed on all stores for tradeId={}, version={}",
                    tradeEntity.getTradeId(), tradeEntity.getVersion());

        } catch (Exception e) {
            log.error("Error during persistence: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to persist trade", e);
        }
    }

    public void expireTradeInNoSqlAndStateStore(TradeExpiredEvent tradeExpiredEvent){
        log.info("Expiring trade with tradeId={}, in NOSQL & STATE stores", tradeExpiredEvent.getTradeId());
        try {
            TradeAuditEntity tradeAuditEntity = tradeMapper.expiryEventToAuditEntity(tradeExpiredEvent);
            TradeState tradeState = tradeMapper.expiryEventToStateEntity(tradeExpiredEvent);

            //1. UPDATE NO SQL AUDIT STORE
            updateNoSqlStore(tradeAuditEntity);
            log.info("Trade expiry updated in No SQL Store for : trade id={} ",tradeState.getTradeId());

            //2. UPDATE STATE STORE VIA KAFKA STREAMS
            publishUpdateEventForTradeStateStore(tradeState);
            log.info("Trade expiry updated in State Store (via Kafka Streams) for : trade id={} ",tradeState.getTradeId());

        } catch(Exception e){
            log.error("Error during expiry event writing to NoSql / State store: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write expire event in NoSql/State store", e);
        }
    }

    private void updateSqlStore(TradeEntity entity){
        log.info("Persisting trade in SQL Store : tradeId={}, version={}",entity.getTradeId(), entity.getVersion());
        entity.setUpdatedAt(LocalDateTime.now());

        //Conditional update if incoming version >= existing version
        int updated = tradeEntityRepository.updateIfNewer(entity);
        if(updated > 0){
            log.info("Successfully updated trade in SQl store: tradeId={}, version={}",entity.getTradeId(), entity.getVersion());
            return;
        }
        log.debug("0 rows updated for trade in SQl store: tradeId={}, version={}",entity.getTradeId(), entity.getVersion());

        //If 0 rows updated, then it should be an insert
        entity.setCreatedAt(LocalDateTime.now());
        try{
            entity = tradeEntityRepository.save(entity);
            log.debug("Inserted row for trade in SQl store: tradeId={}, version={}",entity.getTradeId(), entity.getVersion());
        } catch(Exception e){
            log.warn("Unable to insert row for trade in SQl store: tradeId={}, version={}",entity.getTradeId(), entity.getVersion());
            //May be another thread inserted causing this to fail, attempt update
            int updateOnRetryCount = tradeEntityRepository.updateIfNewer(entity);
            if(updateOnRetryCount > 0){
                log.debug("Updated trade on retry in SQl store: tradeId={}, version={}",entity.getTradeId(), entity.getVersion());
                return;
            }
            log.error("Update/ Insert / Update on retry operations failed in SQl store for trade: tradeId={}, version={}",entity.getTradeId(), entity.getVersion());
            throw e;
        }
    }

    private void updateNoSqlStore(TradeAuditEntity tradeAuditEntity) {
        log.info("Persisting trade audit in NoSQL Store : tradeId={}, version={}",tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion());
        long backOffms = INITIAL_BACKOFF_MS;
        for(int attempt = 1;attempt <= MAX_RETRIES; attempt++){
            try{
                tradeAuditRepository.save(tradeAuditEntity);
                log.info("Successfully persisted trade audit in NoSQL Store : tradeId={}, version={}",tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion());
                log.debug("Trade Audit persisted in No Sql store for trade: tradeId={}, version={}",tradeAuditEntity.getTradeId(),tradeAuditEntity.getVersion());
                return;
            } catch(Exception e){
                log.warn("Write [attempt {}/{}] to NoSql store failed for trade Id {}, version {}", attempt, MAX_RETRIES, tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion(),e);

                if(attempt == MAX_RETRIES){
                    log.error("All {} retry attempts failed for trade Id {}, version {} writes to Nosql store. Should go to DLQ", MAX_RETRIES, tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion(), e);
                    throw new RuntimeException(String.format("Failed to persist trade %s to NoSql after %d retries", tradeAuditEntity.getTradeId(), MAX_RETRIES),e );
                }
                try{
                    log.debug("Waiting {}ms before retry attempt {} for trade: trade Id {}, version : {}", backOffms, attempt+1, tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion());
                    Thread.sleep(backOffms);
                    backOffms = Math.min ( (long) (backOffms * BACKOFF_MULTIPLIER), MAX_BACKOFF_MS);
                } catch (InterruptedException ie){
                   log.error("Retry sleep interrupted for trade : trade Id {}, version {}", tradeAuditEntity.getTradeId(), tradeAuditEntity.getVersion());
                   Thread.currentThread().interrupt();
                   throw new RuntimeException("NoSql retry interrupted",ie);
                }
            }
        }
    }

    private void publishUpdateEventForTradeStateStore(TradeState tradeState) {
        tradeStateService.publishUpdateEventForTradeStateStore(tradeState);
        log.debug("Persisted in Trade State Store : tradeId={}, version={}", tradeState.getTradeId(), tradeState.getLastProcessedVersion());
    }
}
