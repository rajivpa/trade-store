package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import com.dws.tradestore.processor.service.TradeProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Slf4j
@Service
public class TradeReceivedEventConsumer {

    @Autowired
    private TradeProcessingService tradeProecssor;

    /**
     * Consumes trade received events from Kafka and delegates to the processor.
     * We are using Manual Acknowledgements for kafka messages.
     * - Message is only marked as consumed after successfull processing.
     * - If processing fails, message is NOT acknowledged and will be retried.
     * @param tradeRcvdEvent
     * @param topic
     * @param partition
     * @param offset
     * @param key
     * @param ack
     * @param record
     */
    @KafkaListener(topics = "${app.kafka.topics.trades}", groupId = "${app.kafka.consumer-groups.trades-processor}")
    public void onTradeReceived(@Payload TradeReceivedEvent tradeRcvdEvent,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
                          @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
                          @Header(name = KafkaHeaders.OFFSET, required = false) Long offset,
                          @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                          Acknowledgment ack,
                          ConsumerRecord<String, TradeReceivedEvent> record) {

        try{
            //If null message received, log it & ack it
            if(tradeRcvdEvent==null){
                log.warn("Received null TradeReceiveEvent, no further processing. topic={}, partition={}, offset={}, key={}",
                        topic, partition, offset, key);
                ack.acknowledge();
                return;
            }
            log.info("Received TradeReceivedEvent topic={}, partition={}, offset={}, key={}",topic, partition, offset, key);

            //Delegate all the processing to tradeProcessor
            tradeProecssor.processTradeEvent(tradeRcvdEvent);

            //Acknowledge only after successful processing
            ack.acknowledge();

            log.info("Processed TradeReceivedEvent topic={}, partition={}, offset={}, key={}",topic, partition, offset, key);

        } catch(Exception e){
            log.error("Failed to process TradeReceivedEvent with eventId={}, tradeId={}. version={}",
                    tradeRcvdEvent.getEventId(), tradeRcvdEvent.getTradeId(),tradeRcvdEvent.getVersion());
            //Re-throw error to prevent acknowledgement, so that message can be retired by Kafka consumer
            throw e;
        }
    }
}
