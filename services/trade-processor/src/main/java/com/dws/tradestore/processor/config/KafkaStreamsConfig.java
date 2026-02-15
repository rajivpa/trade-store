package com.dws.tradestore.processor.config;

import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    public static final String TRADE_STATE_STORE = "trade-state-store";

    @Value("${app.kafka.topics.tradeStateUpdates}")
    private String tradeStateUpdatesTopic;

    @Value("${app.kafka.topics.trades}")
    private String tradesTopic;

    @Bean
    public ObjectMapper objectMapper(){
        return new ObjectMapper();
    }

    @Bean
    public KStream<String, TradeReceivedEvent> kStream(StreamsBuilder streamsBuilder, ObjectMapper objectMapper){

        JsonSerde<TradeState> tradeStateSerde = new JsonSerde<>(TradeState.class, objectMapper);

        // Materialize state update events into a local RocksDB-backed queryable store.
        KTable<String, TradeState> stateStoreTable = streamsBuilder.table(
                tradeStateUpdatesTopic,
                Consumed.with(Serdes.String(), tradeStateSerde),
                Materialized.<String, TradeState, KeyValueStore<Bytes, byte[]>>as(TRADE_STATE_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(tradeStateSerde)
        );

        stateStoreTable.toStream()
                .peek((key, value) -> {
                    if (value != null) {
                        log.info("Kafka streams - Updating state store: tradeId {}, version={}",
                                value.getTradeId(), value.getLastProcessedVersion());
                    }
                });

        return streamsBuilder.stream(
                tradesTopic,
                Consumed.with(
                Serdes.String(),
                new JsonSerde<>(TradeReceivedEvent.class, objectMapper)
                )
        );
    }
}
