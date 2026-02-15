package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import com.dws.tradestore.processor.service.TradeExpirationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TradeExpiredEventConsumer {

    @Autowired
    private TradeExpirationService tradeExpirationService;

    @KafkaListener(topics = "${app.kafka.topics.tradeExpired}", groupId = "${app.kafka.consumer-groups.trade-expiration}")
    public void onTradeExpired(TradeExpiredEvent tradeExpiredEvent, Acknowledgment ack){
        try{
            if(tradeExpiredEvent == null){
                log.error("Received null expired event from Kafka");
                ack.acknowledge();
                return;
            }

            tradeExpirationService.processExpiredEventForAuditAndStateStore(tradeExpiredEvent);

            //Acknowledge only after successful processing
            ack.acknowledge();

            log.info("Processed Expire trade event : trade Id {}", tradeExpiredEvent.getTradeId());

        } catch (Exception e){
            log.error("Error processing expired event",e);
        }
    }
}
