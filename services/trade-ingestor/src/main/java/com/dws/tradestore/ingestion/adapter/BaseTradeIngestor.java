package com.dws.tradestore.ingestion.adapter;

import com.dws.tradestore.ingestion.dto.TradeRequest;
import com.dws.tradestore.ingestion.service.TradeReceivedEventKafkaPublisher;
import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import com.dws.tradestore.ingestion.event.TradeRecievedEvent;
import com.dws.tradestore.ingestion.event.enums.RejectionStage;
import com.dws.tradestore.ingestion.validation.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j
public abstract class BaseTradeIngestor implements ITradeIngestor {

    @Autowired
    protected TradeReceivedEventKafkaPublisher kafkaPublishService;

    @Override
    public void processTradeRequest(TradeRequest request){
        log.info("Processing trade from channel: {}", getSourceChannel());
        try {
            if(request==null) {
                log.error("Received null trade from channel: {}, throwing error", getSourceChannel());
                throw new IllegalArgumentException("Trade request cannot be null");
            }

            log.debug("Starting structural validations of the incoming trade request with Id {}",request.getTradeId());
            //1. Validate the schema of incoming request
            ValidationResult result = validateTrade(request);

            //2. Process rejected request & stop
            if (!result.isValid()) {
                log.warn("Incoming trade request with Id {}, failed validation checks: {}",request.getTradeId(),result.getErrorMessage());
                handleRejection(request, result);
                throw new IllegalArgumentException(result.getErrorMessage());
            }

            log.debug("Starting transformation of incoming trade request with Id: {}, to canonical form", request.getTradeId());
            //3. Valid requests - Normalize/transform to Canonical form
            TradeRecievedEvent trade = transformTrade(request);

            log.debug("Proceeding with publishing of incoming trade request with Id: {}, to streaming service",  request.getTradeId());
            //4. Publish to streaming service for further processing.
            kafkaPublishService.publishTradeReceivedEvent(trade);

            log.info("Completed with ingestion workflow for the incoming trade request with Id: {}",request.getTradeId());
        } catch(Exception e){
            log.error("Error while processing trade from channel {}",getSourceChannel(),e);
            throw e;
        }
    }

    protected void handleRejection(TradeRequest request, ValidationResult result){
        RejectedTradeEvent rejectedTrade = RejectedTradeEvent.builder()
                .tradeId(request.getTradeId())
                .version(request.getVersion())
                .sourceChannel(getSourceChannel())
                .stage(RejectionStage.INGESTION)
                .rejectionReason(result.getErrorMessage())
                .rawPayload(request)
                .ingestionTimestamp(LocalDateTime.now())
                .rejectionTimestamp(LocalDateTime.now())
                .build();
        kafkaPublishService.publishIngestionRejectedEvent(rejectedTrade);
    }

    protected void publishToTradeTopic(TradeRecievedEvent trade){
        kafkaPublishService.publishTradeReceivedEvent(trade);
    }

    protected abstract ValidationResult validateTrade(TradeRequest request);

    protected abstract TradeRecievedEvent transformTrade(TradeRequest request);
}
