package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TradeExpiryPublisher {

    @Value("${app.kafka.topics.tradeExpired}")
    private String tradeExpiredTopic;

    @Autowired
    private KafkaTemplate<String, TradeExpiredEvent> kafkaTemplate;

    public void publishTradeExpiredEvent(TradeExpiredEvent tradeExpiredEvent){
        log.info("Preparing to publish the trade expiry event for Trade Id : {} and version {}", tradeExpiredEvent.getTradeId(),tradeExpiredEvent.getVersion());
        try{
            kafkaTemplate.send(tradeExpiredTopic, tradeExpiredEvent.getTradeId(), tradeExpiredEvent);
            log.info("Successfully published the trade reject event for Trade Id : {} and version {}", tradeExpiredEvent.getTradeId(),tradeExpiredEvent.getVersion());
        } catch(Exception e){
            log.error("Error while publishing the trade reject event for Trade Id : {} and version {}", tradeExpiredEvent.getTradeId(),tradeExpiredEvent.getVersion(), e);
            throw new RuntimeException("Failed to publish trade expiry event", e);
        }
    }
}
