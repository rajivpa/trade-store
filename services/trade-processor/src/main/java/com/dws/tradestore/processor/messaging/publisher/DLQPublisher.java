package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DLQPublisher {

    @Value("${app.kafka.topics.tradesDlq}")
    private String dlqTopic;
    
    @Autowired
    private KafkaTemplate<String, TradeReceivedEvent> kafkaTemplate;

    public void publishToDeadLetterQueue(TradeReceivedEvent event){
        try{
            kafkaTemplate.send(dlqTopic, event.getEventId(), event);
            log.info("Trade {} published to DLQ topic.",event.getTradeId());
        } catch(Exception e){
            log.error("Error publishing trade {} to DLQ topic: {}",event.getTradeId(), e);
        }
    }
    
}
