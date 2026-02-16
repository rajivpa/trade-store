package com.dws.tradestore.ingestion.service;

import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import com.dws.tradestore.ingestion.event.enums.TransmissionChannel;
import com.dws.tradestore.ingestion.exception.KafkaPublishException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DlqPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private DlqPublisher dlqPublisher;

    private RejectedTradeEvent rejectedTrade;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(dlqPublisher, "dlqTopic", "trade-ingestor-dlq");
        ReflectionTestUtils.setField(dlqPublisher, "producerTimeoutMillis", 5000L);

        rejectedTrade = RejectedTradeEvent.builder()
                .tradeId("T1")
                .version(1)
                .rejectionReason("Missing trade id")
                .sourceChannel(TransmissionChannel.REST_API)
                .build();
    }

    @Test
    void publishRejectedTrade_Success() {
        SendResult<String, Object> sendResult = mock(SendResult.class);
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        dlqPublisher.publishRejectedTrade(rejectedTrade, new RuntimeException("primary publish failed"));

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaTemplate).send(messageCaptor.capture());
        Message<?> capturedMessage = messageCaptor.getValue();
        assertThat(capturedMessage.getPayload()).isEqualTo(rejectedTrade);
        assertThat(capturedMessage.getHeaders().get("kafka_topic")).isEqualTo("trade-ingestor-dlq");
        assertThat(capturedMessage.getHeaders().get("kafka_messageKey")).isEqualTo("T1");
    }

    @Test
    void publishRejectedTrade_Failure_ThrowsKafkaPublishException() {
        CompletableFuture<SendResult<String, Object>> future =
                CompletableFuture.failedFuture(new TimeoutException("Kafka unavailable"));
        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        assertThatThrownBy(() -> dlqPublisher.publishRejectedTrade(rejectedTrade, new RuntimeException("publish failure")))
                .isInstanceOf(KafkaPublishException.class)
                .hasMessageContaining("Failed to publish rejected trade to DLQ topic");
    }
}
