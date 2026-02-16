package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class DlqPublisher {

    @Value("${app.kafka.topics.tradesDlq}")
    private String dlqTopic;

    @Value("${app.kafka.producer.publish-timeout-ms:5000}")
    private long publishTimeoutMs;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishToDeadLetterQueue(TradeReceivedEvent event, Throwable cause){
        try{
            Message<TradeReceivedEvent> dlqMessage = MessageBuilder
                    .withPayload(event)
                    .setHeader(KafkaHeaders.TOPIC, dlqTopic)
                    .setHeader(KafkaHeaders.KEY, event.getTradeId())
                    .setHeader("dlq-cause", cause == null ? "n/a" : String.valueOf(cause.getMessage()))
                    .build();

            kafkaTemplate.send(dlqMessage).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
            log.info("Published trade with tradeId {} to DLQ topic {}", event.getTradeId(), dlqTopic);
        } catch(Exception e){
            throw new RuntimeException("Failed to publish trade to DLQ topic", e);
        }
    }
}
