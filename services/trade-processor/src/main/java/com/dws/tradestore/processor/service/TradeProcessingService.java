package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.messaging.publisher.DLQPublisher;
import com.dws.tradestore.processor.messaging.publisher.TradeRejectedEventPublisher;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Slf4j
@Service
public class TradeProcessingService {

    @Autowired
    private TradeStateStoreService tradeStateStoreService;

    @Autowired
    private ValidationService tradeValidationService;

    @Autowired
    private TradeWriteService tradeWriteService;

    @Autowired
    private TradeRejectedEventPublisher rejectedEventPublisher;

    @Autowired
    private DLQPublisher dlqPublisher;

    @Autowired
    private TradeMapper tradeMapper;

    /**
     * Orchestrates the complete trade processing flow. This a stateful processing.
     * 1. Check the received trade against fast storage & gets its current state if exists
     * 2. Perform business validations
     * 3. Delegate to TradeWriteService for persistence
     * 4. Handle rejections / errors
     * @param tradeRcvdEvnt
     */
    public void processTradeEvent(TradeReceivedEvent tradeRcvdEvnt){
        log.info("Trade Received Event processing started: tradeId={}, version={}", tradeRcvdEvnt.getTradeId(),
                tradeRcvdEvnt.getVersion());
        try{
            //1. CONVERT EVENT TO DOMAIN TRADE
            log.debug("Converting trade received event with tradeId={}, to domain Trade ", tradeRcvdEvnt.getTradeId());
            Trade trade = tradeMapper.eventToDomain(tradeRcvdEvnt);

            //2. GET LATEST TRADE STATE FROM TRADE STORE
            log.debug("Loading current state for tradeId={}", trade.getTradeId());
            Optional<TradeState> tradeCurrStateOpt = tradeStateStoreService.getTradeState(trade.getTradeId());

            //3. INITIATE BUSINESS VALIDATION
            log.debug("Initiating business validations for Trade Received Event : tradeId={}", trade.getTradeId());
            ValidationResult validationResult = tradeValidationService.validate(trade, tradeCurrStateOpt);
            if(!validationResult.isValid()){
                //trade.setStatus(TradeStatus.REJECTED);
                log.warn("Validations failed for tradeId={} version={}. Reasons={}", tradeRcvdEvnt.getTradeId(),
                        tradeRcvdEvnt.getVersion(), validationResult.getErrors());
                handleRejection(trade, validationResult, tradeCurrStateOpt);
                return;
            }

            //3. PERSIST
            trade.setProcessingTimestamp(LocalDateTime.now());
            log.debug("Commit Trade Received Event : tradeId={} to SQL & NO-SQL stores", tradeRcvdEvnt.getTradeId());
            tradeWriteService.acceptTrade(trade);

            log.info("Completed processing of Trade Received Event: tradeId={}, version={}", tradeRcvdEvnt.getTradeId(),
                    tradeRcvdEvnt.getVersion());

        } catch (Exception e) {
            log.error("Error processing Trade Received Event: tradeId={}, version={}", tradeRcvdEvnt.getTradeId(),
                    tradeRcvdEvnt.getVersion(),e);
            handleError(tradeRcvdEvnt);
            throw e;
        }
    }
    private void handleRejection(
            Trade trade,
            ValidationResult validationResult,
            Optional<TradeState> currentState) {

        rejectedEventPublisher.publishRejectedEvent(trade, validationResult, currentState);
    }

    private void handleError(TradeReceivedEvent event){
        try{
            dlqPublisher.publishToDeadLetterQueue(event);
            log.info("Trade id {} published to DLQ ",event.getTradeId());
        } catch(Exception e){
            log.error("Error publishing to DLQ topic",e);
        }
    }

}
