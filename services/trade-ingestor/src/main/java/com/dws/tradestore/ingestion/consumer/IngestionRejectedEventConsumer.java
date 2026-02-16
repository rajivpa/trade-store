package com.dws.tradestore.ingestion.consumer;

import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class IngestionRejectedEventConsumer {

    @KafkaListener(
            topics = "${kafka.topics.ingestion-rejects}",
            groupId = "${kafka.consumer.groups.ingestion-rejects:ingestion-rejects-group}"
    )
    public void onIngestionRejectEvent(RejectedTradeEvent rejectedEvent) {
        // TODO: This consumer will collect trades rejected during ingestion because of validation failures.
        // TODO: Persist all rejected trades into a separate rejected-trades column family in the NoSQL store.
    }
}
