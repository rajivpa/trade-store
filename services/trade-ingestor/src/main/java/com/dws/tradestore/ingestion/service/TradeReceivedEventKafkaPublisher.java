package com.dws.tradestore.ingestion.service;

import com.dws.tradestore.ingestion.exception.KafkaPublishException;
import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import com.dws.tradestore.ingestion.event.TradeRecievedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Publishes messages to Kafka topics.
 */
@Slf4j
@Service
public class TradeReceivedEventKafkaPublisher implements ITradeReceivedEventPublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private DlqPublisher dlqPublisher;

    @Value("${kafka.topics.trades}")
    private String tradesTopic;

    @Value("${kafka.topics.ingestion-rejects}")
    private String ingestionRejectsTopic;

    @Value("${kafka.producer.timeout-ms}")
    private long producerTimeoutMillis;

    @Value("${kafka.producer.retry-enabled}")
    private boolean retryEnabled;

    @Value("${kafka.producer.max-retries}")
    private int maxRetries;

    @Value("${kafka.producer.retry-backoff-base-ms:1000}")
    private long retryBackoffBaseMillis = 1000L;

    /**
     * Publishes the valid trades to the trades topic
     * TradeId is used as parition key, this ensures ordering of versions per trade
     * @param trade
     */
    public void publishTradeReceivedEvent(TradeRecievedEvent trade){
        log.info("Publishing trade with tradeId: {} & version: {} to trade topic",trade.getTradeId(),trade.getVersion());
        try {
            Message<TradeRecievedEvent> msg = MessageBuilder
                    .withPayload(trade)
                    .setHeader(KafkaHeaders.TOPIC, tradesTopic)
                    .setHeader(KafkaHeaders.KEY, trade.getTradeId())
                    .setHeader("transmission-channel",trade.getTransmissionChannel().name()).build();
            publishToTopic(msg,trade.getTradeId());
            log.info("Completed publishing trade with tradeId: {} & version: {} to trade topic",trade.getTradeId(),trade.getVersion());
        } catch (Exception e) {
            log.error("Error while publishing trade with tradeId: {} & version: {} to trade topic",trade.getTradeId(),trade.getVersion());
            throw new KafkaPublishException("Failed to publish valid trade to Trades topic",e);
        }
    }

    /**
     * Publish the ingetion rejects to appropriate topic
     * @param trade
     */
    public void publishIngestionRejectedEvent(RejectedTradeEvent trade){
        log.info("Publishing ingestion rejected trade with tradeId: {}, to ingestion rejects topic ",trade.getTradeId());
        try {
            Message<RejectedTradeEvent> msg = MessageBuilder
                    .withPayload(trade)
                    .setHeader(KafkaHeaders.TOPIC, ingestionRejectsTopic)
                    .setHeader(KafkaHeaders.KEY, trade.getTradeId()).build();
            publishToTopic(msg,trade.getTradeId());
            log.info("Completed Publishing ingestion rejected trade with tradeId: {}, to ingestion rejects topic ",trade.getTradeId());
        } catch (Exception e) {
            log.error("Error while publishing ingestion rejected trade with tradeId: {}, to ingestion rejects topic ",trade.getTradeId());
            dlqPublisher.publishRejectedTrade(trade, e);
            throw new KafkaPublishException("Failed to publish ingestion rejected trade", e);
        }
    }
    /*
     * Does the actual work of publishing to Kafka topic with retries
     */
    private void publishToTopic(Message msg,String tradeId) throws Exception{

        int attemptNo = 0;
        int maxAttempts = Math.max(1, maxRetries);

        if (maxRetries <= 0) {
            log.warn("Configured kafka.producer.max-retries={} is invalid. Falling back to single publish attempt.", maxRetries);
        }

        log.info("Sending message with tradeId : {} to Kafka",tradeId);

        while(attemptNo < maxAttempts) {
            attemptNo++;
            log.debug("Attempt no {} for message with tradeId : {} ", attemptNo, tradeId);
            try {
                CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(msg);
                future.get(producerTimeoutMillis, TimeUnit.MILLISECONDS);
                log.info("Completed publishing message with trade Id :{} to Kafka",tradeId);
                return;
            } catch (Exception ex) {
                log.error("Error while sending message with tradeId :{} to Kafka: {}", tradeId, ex.getMessage(), ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if(!retryEnabled || attemptNo >= maxAttempts) {
                    log.warn("All attempts exhausted for message with tradeId {}, throwing exception",tradeId);
                    throw new Exception("All attempts to publish failed, max retries exhausted", ex);
                }
                //Calculate delay for exponential back-off in case of re-attempts, pass the next attempt no
                long retryDelay = calculateRetryDelay(attemptNo+1);
                log.debug("Preparing to re-attempt kafka publish. Attempt No {}, delay: {}",attemptNo+1,retryDelay);
                Thread.sleep(retryDelay);
            }
        }
    }

    private long calculateRetryDelay(int attemptNo){
        //Exponential backoff: 1s, 2s, 4s
        return (long)Math.pow(2, attemptNo-1) * retryBackoffBaseMillis;
    }
}
