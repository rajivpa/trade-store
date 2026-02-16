package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.outbound.TradeRejectedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ProcessingRejectedEventConsumer {

    @KafkaListener(
            topics = "${app.kafka.topics.processingRejects}",
            groupId = "${app.kafka.consumer-groups.processing-rejects:processing-rejects-group}"
    )
    public void onProcessingRejectEvent(TradeRejectedEvent rejectedEvent) {
        // TODO: This consumer will collect trades rejected during processing due to business validation failures.
        // TODO: Persist these rejected trades into a separate Rejects column family in the NoSQL store.
    }
}
