package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
import com.dws.tradestore.processor.domain.model.TradeState;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeStateStoreServiceTest {

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Mock
    private KafkaTemplate<String, TradeState> kafkaTemplate;

    @InjectMocks
    private TradeStateStoreService tradeStateStoreService;

    @Test
    void getTradeState_happyPath_foundInStore() throws Exception {
        KafkaStreams kafkaStreams = org.mockito.Mockito.mock(KafkaStreams.class);
        StreamsBuilder streamsBuilder = org.mockito.Mockito.mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        ReadOnlyKeyValueStore<String, TradeState> keyValueStore = org.mockito.Mockito.mock(ReadOnlyKeyValueStore.class);

        when(streamsBuilderFactoryBean.getObject()).thenReturn(streamsBuilder);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        doReturn(keyValueStore).when(kafkaStreams).store(any(StoreQueryParameters.class));
        when(keyValueStore.get("T1")).thenReturn(TradeState.builder()
                .tradeId("T1")
                .lastProcessedVersion(2)
                .status(TradeStatus.ACTIVE)
                .build());

        Optional<TradeState> result = tradeStateStoreService.getTradeState("T1");

        assertTrue(result.isPresent());
        assertTrue("T1".equals(result.get().getTradeId()));
    }

    @Test
    void getTradeState_negative_streamsNotInitialized() throws Exception {
        when(streamsBuilderFactoryBean.getObject()).thenReturn(null);

        Optional<TradeState> result = tradeStateStoreService.getTradeState("T1");

        assertTrue(result.isEmpty());
    }

    @Test
    void publishUpdateEventForTradeStateStore_happyPath() {
        ReflectionTestUtils.setField(tradeStateStoreService, "tradeStateUpdatesTopic", "trade-state-updates");
        ReflectionTestUtils.setField(tradeStateStoreService, "publishTimeoutMs", 5000L);
        TradeState state = TradeState.builder().tradeId("T1").lastProcessedVersion(1).status(TradeStatus.ACTIVE).build();
        SendResult<String, TradeState> sendResult = mock(SendResult.class);
        CompletableFuture<SendResult<String, TradeState>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send("trade-state-updates", "T1", state)).thenReturn(future);

        assertDoesNotThrow(() -> tradeStateStoreService.publishUpdateEventForTradeStateStore(state));

        verify(kafkaTemplate).send("trade-state-updates", "T1", state);
    }

    @Test
    void publishUpdateEventForTradeStateStore_negative_sendFailureThrows() {
        ReflectionTestUtils.setField(tradeStateStoreService, "tradeStateUpdatesTopic", "trade-state-updates");
        ReflectionTestUtils.setField(tradeStateStoreService, "publishTimeoutMs", 5000L);
        TradeState state = TradeState.builder().tradeId("T1").lastProcessedVersion(1).status(TradeStatus.ACTIVE).build();
        when(kafkaTemplate.send(any(), any(), any())).thenThrow(new RuntimeException("kafka down"));

        assertThrows(RuntimeException.class, () -> tradeStateStoreService.publishUpdateEventForTradeStateStore(state));
        verify(kafkaTemplate).send("trade-state-updates", "T1", state);
    }
}
