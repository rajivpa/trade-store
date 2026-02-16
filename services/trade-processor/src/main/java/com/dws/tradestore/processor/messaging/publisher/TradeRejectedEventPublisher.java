package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import com.dws.tradestore.processor.event.outbound.TradeRejectedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class TradeRejectedEventPublisher {

    @Value("${app.kafka.topics.processingRejects}")
    private String processingRejectsTopic;

    @Autowired
    private KafkaTemplate<String, TradeRejectedEvent> kafkaTemplate;

    @Value("${app.kafka.producer.publish-timeout-ms:5000}")
    private long publishTimeoutMs;

    public void publishRejectedEvent(Trade trade,
                                     ValidationResult validationResult,
                                     Optional<TradeState> currentState){
        log.info("Preparing to publish the trade reject event for Trade Id : {} and version {}", trade.getTradeId(),trade.getVersion());
        try{
            TradeRejectedEvent rejectedEvent = buildRejectedTradeEvent(trade, validationResult, currentState);
            kafkaTemplate.send(processingRejectsTopic, rejectedEvent.getTradeId(), rejectedEvent)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
            log.info("Successfully published the trade reject event for Trade Id : {} and version {}", trade.getTradeId(),trade.getVersion());
        } catch(Exception e){
            log.error("Error while publishing the trade reject event for Trade Id : {} and version {}", trade.getTradeId(),trade.getVersion(), e);
            throw new RuntimeException("Failed to publish rejected trade event", e);
        }
    }

    private TradeRejectedEvent buildRejectedTradeEvent(Trade trade, ValidationResult validationResult, Optional<TradeState> currentState){
        TradeRejectedEvent rejectedTradeEvent = TradeRejectedEvent.builder()
                .rejectionId(UUID.randomUUID().toString())
                .tradeId(trade.getTradeId())
                .version(trade.getVersion())
                //.stage("PROCESSING")
                .rejectionReason(validationResult.getErrorMessage())
                .failedValidations(validationResult.getErrors())
                .sourceChannel(trade.getTransmissionChannel().name())
                .ingestionTimestamp(trade.getIngestionTimestamp())
                .rejectionTimestamp(LocalDateTime.now())
                .currentVersion(currentState.map(TradeState::getLastProcessedVersion).orElse(null)).build();

        return rejectedTradeEvent;
    }
}
