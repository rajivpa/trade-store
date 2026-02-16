package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.inbound.TickEvent;
import com.dws.tradestore.processor.service.TradeExpirationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TickConsumer {
    @Autowired
    private TradeExpirationService tradeExpirationService;

    /**
     * This method receives tick events from Kafka and uses them to trigger
     * maturity date check on the Trade SQL store, any trades having maturity date
     * less than asOfDate provided by tick get marked as EXPIRED
     * @param tickEvent
     * @param ack
     */
    @KafkaListener(topics = "${app.kafka.topics.ticks}", groupId = "${app.kafka.consumer-groups.trade-expiration}")
    public void onTickReceived(TickEvent tickEvent, Acknowledgment ack){

        try{
            if(tickEvent == null){
                log.error("Received null tick event from Kafka");
                ack.acknowledge();
                return;
            }

            if(tickEvent.getBusinessDate() == null){
                log.error("Received tick event with null currentDate");
                ack.acknowledge();
                return;
            }

            log.info("Received tick event with tickId : {}, currentDate : {}", tickEvent.getTickId(),
                    tickEvent.getBusinessDate());

            tradeExpirationService.expireTradeAsOf(tickEvent.getBusinessDate());
            ack.acknowledge();

            log.info("Trade expiration completed for tickId : {}", tickEvent.getTickId());

        } catch (Exception e){
            log.error("Error processing tick event",e);
            //send to DLQ
        }
    }
}
