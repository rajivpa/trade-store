package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeExpiryPublisherTest {

    @Mock
    private KafkaTemplate<String, TradeExpiredEvent> kafkaTemplate;

    @InjectMocks
    private TradeExpiryPublisher publisher;

    @Test
    void publishTradeExpiredEvent_happyPath() {
        ReflectionTestUtils.setField(publisher, "tradeExpiredTopic", "trade-expired");
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();

        publisher.publishTradeExpiredEvent(event);

        verify(kafkaTemplate).send("trade-expired", "T1", event);
    }

    @Test
    void publishTradeExpiredEvent_negative_sendFailureThrows() {
        ReflectionTestUtils.setField(publisher, "tradeExpiredTopic", "trade-expired");
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();
        when(kafkaTemplate.send(any(), any(), any())).thenThrow(new RuntimeException("kafka unavailable"));

        assertThrows(RuntimeException.class, () -> publisher.publishTradeExpiredEvent(event));
        verify(kafkaTemplate).send("trade-expired", "T1", event);
    }
}
