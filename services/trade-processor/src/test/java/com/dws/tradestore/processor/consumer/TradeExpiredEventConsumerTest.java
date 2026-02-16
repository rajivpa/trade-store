package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import com.dws.tradestore.processor.service.TradeExpiredEventProcessingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TradeExpiredEventConsumerTest {

    @Mock
    private TradeExpiredEventProcessingService tradeExpiredEventProcessingService;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private TradeExpiredEventConsumer consumer;

    @Test
    void onTradeExpired_happyPath() {
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();

        consumer.onTradeExpired(event, acknowledgment);

        verify(tradeExpiredEventProcessingService).processExpiredEventForAuditAndStateStore(event);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onTradeExpired_negative_nullEvent() {
        consumer.onTradeExpired(null, acknowledgment);

        verify(tradeExpiredEventProcessingService, never()).processExpiredEventForAuditAndStateStore(org.mockito.ArgumentMatchers.any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onTradeExpired_negative_serviceFailure_ackNotCalled() {
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();
        doThrow(new RuntimeException("failed")).when(tradeExpiredEventProcessingService)
                .processExpiredEventForAuditAndStateStore(event);

        consumer.onTradeExpired(event, acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }
}
