package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DLQPublisherTest {

    @Mock
    private KafkaTemplate<String, TradeReceivedEvent> kafkaTemplate;

    @InjectMocks
    private DLQPublisher publisher;

    @Test
    void publishToDeadLetterQueue_happyPath() {
        ReflectionTestUtils.setField(publisher, "dlqTopic", "trades-dlq");
        TradeReceivedEvent event = TradeReceivedEvent.builder().eventId("E1").tradeId("T1").build();

        publisher.publishToDeadLetterQueue(event);

        verify(kafkaTemplate).send("trades-dlq", "E1", event);
    }

    @Test
    void publishToDeadLetterQueue_negative_sendFailureIsHandled() {
        ReflectionTestUtils.setField(publisher, "dlqTopic", "trades-dlq");
        TradeReceivedEvent event = TradeReceivedEvent.builder().eventId("E1").tradeId("T1").build();
        when(kafkaTemplate.send(any(), any(), any())).thenThrow(new RuntimeException("kafka unavailable"));

        assertDoesNotThrow(() -> publisher.publishToDeadLetterQueue(event));
        verify(kafkaTemplate).send("trades-dlq", "E1", event);
    }
}

