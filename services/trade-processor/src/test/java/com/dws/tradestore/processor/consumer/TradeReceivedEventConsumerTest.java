package com.dws.tradestore.processor.consumer;

import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import com.dws.tradestore.processor.service.TradeProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TradeReceivedEventConsumerTest {

    @Mock
    private TradeProcessingService tradeProecssor;

    @Mock
    private Acknowledgment acknowledgment;

    @SuppressWarnings("unchecked")
    @Mock
    private ConsumerRecord<String, TradeReceivedEvent> record;

    @InjectMocks
    private TradeReceivedEventConsumer consumer;

    @Test
    void onTradeReceived_happyPath() {
        TradeReceivedEvent event = TradeReceivedEvent.builder().eventId("E1").tradeId("T1").version(1).build();

        consumer.onTradeReceived(event, "trades", 0, 10L, "k1", acknowledgment, record);

        verify(tradeProecssor).processTradeEvent(event);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onTradeReceived_negative_nullEvent_acknowledges() {
        consumer.onTradeReceived(null, "trades", 0, 10L, "k1", acknowledgment, record);

        verify(tradeProecssor, never()).processTradeEvent(org.mockito.ArgumentMatchers.any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onTradeReceived_negative_processingFails_rethrowsAndNoAck() {
        TradeReceivedEvent event = TradeReceivedEvent.builder().eventId("E1").tradeId("T1").version(1).build();
        doThrow(new RuntimeException("processing failed")).when(tradeProecssor).processTradeEvent(event);

        assertThrows(RuntimeException.class,
                () -> consumer.onTradeReceived(event, "trades", 0, 10L, "k1", acknowledgment, record));

        verify(acknowledgment, never()).acknowledge();
    }
}

