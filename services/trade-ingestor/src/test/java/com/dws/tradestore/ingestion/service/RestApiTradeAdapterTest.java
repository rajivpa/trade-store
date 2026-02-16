package com.dws.tradestore.ingestion.service;

import com.dws.tradestore.ingestion.adapter.rest.RestApiTradeAdapter;
import com.dws.tradestore.ingestion.dto.TradeRequest;
import com.dws.tradestore.ingestion.event.TradeRecievedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RestApiTradeAdapterTest {
    @Mock
    private TradeReceivedEventKafkaPublisher kafkaPublishService;

    @InjectMocks
    private RestApiTradeAdapter restTradeAdapter;

    private TradeRequest validTradeRequest;

    @BeforeEach
    void setUp(){
        validTradeRequest = TradeRequest.builder()
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP-1")
                .bookId("B1")
                .maturityDate("2026-02-08")
                .build();
    }

    @Test
    void testProcessTradeRequest_Success(){
        doNothing().when(kafkaPublishService).publishTradeReceivedEvent(any(TradeRecievedEvent.class));
        restTradeAdapter.processTradeRequest(validTradeRequest);

        ArgumentCaptor<TradeRecievedEvent> tradeCaptor = ArgumentCaptor.forClass(TradeRecievedEvent.class);
        verify(kafkaPublishService).publishTradeReceivedEvent(tradeCaptor.capture());

        TradeRecievedEvent capturedTrade = tradeCaptor.getValue();
        assertThat(capturedTrade.getTradeId()).isEqualTo("T1");
        assertThat(capturedTrade.getVersion()).isEqualTo(1);
    }

    @Test
    void testProcessTradeRequest_InvalidTrade_PublishToIngestionRejectTopic(){
        TradeRequest invalidTradeReq = TradeRequest.builder()
                        .version(1)
                        .counterPartyId("CP-1")
                        .bookId("B1")
                        .maturityDate("2026-02-08").build();

        doNothing().when(kafkaPublishService).publishIngestionRejectedEvent(any());
        assertThatThrownBy(() -> restTradeAdapter.processTradeRequest(invalidTradeReq))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing trade id");

        verify(kafkaPublishService).publishIngestionRejectedEvent(any());
        verify(kafkaPublishService, never()).publishTradeReceivedEvent(any());
    }
}
