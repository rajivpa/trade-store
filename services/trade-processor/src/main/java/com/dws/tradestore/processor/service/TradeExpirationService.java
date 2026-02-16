package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.event.inbound.TickEvent;
import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.messaging.publisher.TradeExpiryPublisher;
import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.repository.TradeAuditRepository;
import com.dws.tradestore.processor.persistence.repository.TradeEntityRepository;
import jnr.constants.platform.Local;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TradeExpirationService {

    @Autowired
    private TradeEntityRepository tradeEntityRepository;

    @Autowired
    private TradeWriteService tradeWriteService;

    @Autowired
    private TradeMapper tradeMapper;

    @Autowired
    private TradeExpiryPublisher tradeExpiryPublisher;

    private static final int PAGE_SIZE = 100;
    @Autowired
    private TradeStateStoreService tradeStateStoreService;

    /**
     * Expires matured trades for a given as-of date. This is triggered via Tick events coming through Kafka
     * @param asOfDate
     */
    @Transactional
    public void expireTradeAsOf(LocalDate asOfDate){
        try{
            if(asOfDate == null)
                throw new IllegalArgumentException("asOfDate must not be null");

            long totalProcessed = 0;
            int batchNumber = 0;

            while(true){
                // Always query first page because processed rows are marked expired and fall out of this result set.
                Pageable pageable = PageRequest.of(0, PAGE_SIZE);
                Page<TradeEntity> expiredTradesPage = tradeEntityRepository.findExpiredTradesPageable(asOfDate, pageable);
                if(expiredTradesPage.isEmpty()){
                   log.info("No trades to expire for date {}. Total processed: {}", asOfDate, totalProcessed);
                   break;
                }
                List<TradeEntity> tradeIdList = expiredTradesPage.getContent();

                //UPDATE SQL STORE
                int updatedRows = bulkExpireTradesInSqlStore(tradeIdList, batchNumber);
                if(updatedRows <= 0){
                    log.warn("No rows updated for batch {}. Stopping expiration loop to avoid endless processing.", batchNumber);
                    break;
                }
                totalProcessed += updatedRows;

                //PUBLISH EXPIRE EVENT FOR NOSQL STORE & STATE STORE
                publishExpiryEvents(tradeIdList);
                batchNumber++;
            }
            log.info("Trade expiration process compeleted for date: {}",asOfDate);

        } catch(Exception e){
            log.error("Error during trade expiration process for date: {}",asOfDate, e);
            throw new RuntimeException("Error during trade expiration", e);
        }
    }

    /**
     * Triggered from expiration events (sent after updatin SQL store) listener, on reciept
     * of expiration event we should update the Audit store and State store.
      * @param tradeExpiredEvent
     */
    public void processExpiredEventForAuditAndStateStore(TradeExpiredEvent tradeExpiredEvent){
        try {
           tradeWriteService.expireTradeInNoSqlAndStateStore(tradeExpiredEvent);
        } catch (Exception e){
            log.error("Error during expiring trade in No SQL store/ State store for tradeId: {}",tradeExpiredEvent.getTradeId(), e);
            throw new RuntimeException("Error during saving of expired trade event", e);
        }
    }

    /**
     * Update the SQL store and mark the events with maturityDate< given date, as expired.
     * Do the updates in bulk.
     * @param tradesToExpire
     * @param pageNumber
     */
    @Transactional
    protected int bulkExpireTradesInSqlStore( List<TradeEntity> tradesToExpire, int pageNumber){
        List<String> tradeIds = tradesToExpire.stream()
                .map(TradeEntity::getTradeId)
                .collect(Collectors.toList());
        log.info("Processing page {} with {} trades",pageNumber, tradeIds.size());
        int updatedRows = tradeEntityRepository.bulkExpireTradesById(tradeIds);
        log.info("Bulk expired {} trades in SQL store for page {}", updatedRows, pageNumber);
        return updatedRows;
    }

    /**
     * Send the trade expired events to Kafka topic, at other end the consumer should udpate the
     * No SQL store & State store and mark the given trades as EXPIRED.
     * @param tradesToExpire
     */
    private void publishExpiryEvents( List<TradeEntity> tradesToExpire){
        for(TradeEntity trade: tradesToExpire){
            try{
                TradeExpiredEvent event = tradeMapper.tradeEntityToExpiredEvent(trade);
                tradeExpiryPublisher.publishTradeExpiredEvent(event);
            }catch(Exception e){
                // NOTE: Current design prioritizes throughput over strict SQL+Kafka consistency.
                // If publish fails after SQL expiry update, some expiry events may be missed.
                // Future improvement options: end-of-day reconciliation or outbox pattern.
                log.error("Error publishing the trade expiry event for trade Id {}", trade.getTradeId());
            }
        }
    }
}
