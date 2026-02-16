package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DlqPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private DlqPublisher publisher;

    @Test
    void publishToDeadLetterQueue_happyPath() {
        ReflectionTestUtils.setField(publisher, "dlqTopic", "trades-dlq");
        ReflectionTestUtils.setField(publisher, "publishTimeoutMs", 5000L);
        TradeReceivedEvent event = TradeReceivedEvent.builder().eventId("E1").tradeId("T1").build();
        SendResult<String, Object> sendResult = mock(SendResult.class);
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        publisher.publishToDeadLetterQueue(event, new RuntimeException("processing failed"));

        verify(kafkaTemplate).send(any(Message.class));
    }

    @Test
    void publishToDeadLetterQueue_negative_sendFailureThrows() {
        ReflectionTestUtils.setField(publisher, "dlqTopic", "trades-dlq");
        ReflectionTestUtils.setField(publisher, "publishTimeoutMs", 5000L);
        TradeReceivedEvent event = TradeReceivedEvent.builder().eventId("E1").tradeId("T1").build();
        when(kafkaTemplate.send(any(Message.class))).thenThrow(new RuntimeException("kafka unavailable"));

        assertThrows(RuntimeException.class, () -> publisher.publishToDeadLetterQueue(event, new RuntimeException("processing failed")));
        verify(kafkaTemplate).send(any(Message.class));
    }
}
