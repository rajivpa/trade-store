package com.dws.tradestore.ingestion.service;

import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import com.dws.tradestore.ingestion.exception.KafkaPublishException;
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

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${kafka.producer.timeout-ms}")
    private long producerTimeoutMillis;

    public void publishRejectedTrade(RejectedTradeEvent rejectedTrade, Throwable cause) {
        try {
            Message<RejectedTradeEvent> dlqMessage = MessageBuilder
                    .withPayload(rejectedTrade)
                    .setHeader(KafkaHeaders.TOPIC, dlqTopic)
                    .setHeader(KafkaHeaders.KEY, rejectedTrade.getTradeId())
                    .setHeader("dlq-cause", cause == null ? "n/a" : String.valueOf(cause.getMessage()))
                    .build();

            kafkaTemplate.send(dlqMessage).get(producerTimeoutMillis, TimeUnit.MILLISECONDS);
            log.info("Published rejected trade with tradeId {} to DLQ topic {}", rejectedTrade.getTradeId(), dlqTopic);
        } catch (Exception e) {
            throw new KafkaPublishException("Failed to publish rejected trade to DLQ topic", e);
        }
    }
}
