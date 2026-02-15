package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.config.KafkaStreamsConfig;
import com.dws.tradestore.processor.domain.model.TradeState;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class TradeStateStoreService {

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    private KafkaTemplate<String, TradeState> kafkaTemplate;

    @Value("${app.kafka.topics.tradeStateUpdates}")
    private String tradeStateUpdatesTopic;

    /**
     * Fetch latest known trade state for a tradeId from the state store.
     */
    public Optional<TradeState> getTradeState(String tradeId) {
        try{
            if(streamsBuilderFactoryBean == null || streamsBuilderFactoryBean.getObject() == null){
                log.warn("Kafka streams not initialized yet");
                return Optional.empty();
            }

            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if(kafkaStreams == null){
                log.warn("KafkaStreams instance is null");
                return Optional.empty();
            }

            ReadOnlyKeyValueStore<String, TradeState> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                        KafkaStreamsConfig.TRADE_STATE_STORE,
                        QueryableStoreTypes.keyValueStore()
                    )
            );

            TradeState tradeState = store.get(tradeId);
            if(tradeState != null){
                log.debug("Found trade state for trade Id: {} & version: {}", tradeId, tradeState.getTradeId());
                return Optional.of(tradeState);
            }

            log.debug("No trade state found for : tradeId={}", tradeId);
            return Optional.empty();

        } catch(Exception e){
            log.error("Error retrieving trade state for tradeId: {}",tradeId);
            return Optional.empty();
        }
    }

    public void publishUpdateEventForTradeStateStore(TradeState tradeState){
        try{
            kafkaTemplate.send(tradeStateUpdatesTopic, tradeState.getTradeId(), tradeState);
            log.info("Trade state update event published for tradeId: {}, version: {} ", tradeState.getTradeId(),tradeState.getLastProcessedVersion());
        } catch(Exception e){
            log.error("Error updating trade state for tradeId: {}", tradeState.getTradeId(),e);
            throw new RuntimeException("Failed to publish trade state update event", e);
        }
    }

}
