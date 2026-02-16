package com.dws.tradestore.processor.messaging.publisher;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import com.dws.tradestore.processor.event.enums.TransmissionChannel;
import com.dws.tradestore.processor.event.outbound.TradeRejectedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeRejectedEventPublisherTest {

    @Mock
    private KafkaTemplate<String, TradeRejectedEvent> kafkaTemplate;

    @InjectMocks
    private TradeRejectedEventPublisher publisher;

    @Test
    void publishRejectedEvent_happyPath() {
        ReflectionTestUtils.setField(publisher, "processingRejectsTopic", "processing-rejects");
        ReflectionTestUtils.setField(publisher, "publishTimeoutMs", 5000L);

        Trade trade = sampleTrade();
        ValidationResult validationResult = ValidationResult.failure("validation failed");
        TradeState currentState = TradeState.builder().tradeId("T1").lastProcessedVersion(2).build();
        ArgumentCaptor<TradeRejectedEvent> eventCaptor = ArgumentCaptor.forClass(TradeRejectedEvent.class);
        SendResult<String, TradeRejectedEvent> sendResult = mock(SendResult.class);
        CompletableFuture<SendResult<String, TradeRejectedEvent>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(future);

        publisher.publishRejectedEvent(trade, validationResult, Optional.of(currentState));

        verify(kafkaTemplate).send(org.mockito.ArgumentMatchers.eq("processing-rejects"),
                org.mockito.ArgumentMatchers.eq("T1"), eventCaptor.capture());
        TradeRejectedEvent sent = eventCaptor.getValue();
        assertNotNull(sent.getRejectionId());
        assertEquals("T1", sent.getTradeId());
        assertEquals(1, sent.getVersion());
        assertEquals(2, sent.getCurrentVersion());
        assertEquals(TransmissionChannel.REST_API.name(), sent.getSourceChannel());
    }

    @Test
    void publishRejectedEvent_negative_sendFailureThrows() {
        ReflectionTestUtils.setField(publisher, "processingRejectsTopic", "processing-rejects");
        ReflectionTestUtils.setField(publisher, "publishTimeoutMs", 5000L);
        Trade trade = sampleTrade();
        ValidationResult validationResult = ValidationResult.failure("validation failed");
        when(kafkaTemplate.send(any(), any(), any())).thenThrow(new RuntimeException("kafka unavailable"));

        assertThrows(RuntimeException.class,
                () -> publisher.publishRejectedEvent(trade, validationResult, Optional.empty()));
    }

    private Trade sampleTrade() {
        return Trade.builder()
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP1")
                .bookId("B1")
                .maturityDate(LocalDate.now().plusDays(2))
                .createdDate(LocalDate.now())
                .transmissionChannel(TransmissionChannel.REST_API)
                .ingestionTimestamp(LocalDateTime.now())
                .build();
    }
}
