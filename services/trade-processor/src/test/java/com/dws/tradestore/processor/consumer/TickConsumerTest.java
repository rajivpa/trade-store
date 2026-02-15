package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.inbound.TickEvent;
import com.dws.tradestore.processor.service.TradeExpirationService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.LocalDate;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TickConsumerTest {

    @Mock
    private TradeExpirationService tradeExpirationService;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private TickConsumer tickConsumer;

    @Test
    void onTickReceived_happyPath() {
        TickEvent event = new TickEvent("tick-1", LocalDate.now());

        tickConsumer.onTickReceived(event, acknowledgment);

        verify(tradeExpirationService).expireTradeAsOf(event.getBusinessDate());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onTickReceived_negative_nullEvent() {
        tickConsumer.onTickReceived(null, acknowledgment);

        verify(tradeExpirationService, never()).expireTradeAsOf(org.mockito.ArgumentMatchers.any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onTickReceived_negative_nullBusinessDate() {
        TickEvent event = new TickEvent("tick-1", null);

        tickConsumer.onTickReceived(event, acknowledgment);

        verify(tradeExpirationService, never()).expireTradeAsOf(org.mockito.ArgumentMatchers.any());
        verify(acknowledgment).acknowledge();
    }
}
