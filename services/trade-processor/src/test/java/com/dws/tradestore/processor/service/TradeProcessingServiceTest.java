package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.messaging.publisher.DLQPublisher;
import com.dws.tradestore.processor.messaging.publisher.TradeRejectedEventPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeProcessingServiceTest {

    @Mock
    private TradeStateStoreService tradeStateStoreService;

    @Mock
    private ValidationService tradeValidationService;

    @Mock
    private TradeWriteService tradeWriteService;

    @Mock
    private TradeRejectedEventPublisher rejectedEventPublisher;

    @Mock
    private DLQPublisher dlqPublisher;

    @Mock
    private TradeMapper tradeMapper;

    @InjectMocks
    private TradeProcessingService tradeProcessingService;

    @Test
    void processTradeEvent_happyPath_validTrade() {
        TradeReceivedEvent event = sampleEvent();
        Trade trade = sampleTrade();

        when(tradeMapper.eventToDomain(event)).thenReturn(trade);
        when(tradeStateStoreService.getTradeState("T1")).thenReturn(Optional.empty());
        when(tradeValidationService.validate(trade, Optional.empty())).thenReturn(ValidationResult.success());

        tradeProcessingService.processTradeEvent(event);

        verify(tradeWriteService).acceptTrade(trade);
        verify(rejectedEventPublisher, never()).publishRejectedEvent(any(), any(), any());
        verify(dlqPublisher, never()).publishToDeadLetterQueue(any());
    }

    @Test
    void processTradeEvent_negative_validationFailure() {
        TradeReceivedEvent event = sampleEvent();
        Trade trade = sampleTrade();
        ValidationResult failed = ValidationResult.failure("invalid version");

        when(tradeMapper.eventToDomain(event)).thenReturn(trade);
        when(tradeStateStoreService.getTradeState("T1")).thenReturn(Optional.of(TradeState.builder()
                .tradeId("T1")
                .lastProcessedVersion(2)
                .build()));
        when(tradeValidationService.validate(any(), any())).thenReturn(failed);

        tradeProcessingService.processTradeEvent(event);

        verify(rejectedEventPublisher).publishRejectedEvent(any(), any(), any());
        verify(tradeWriteService, never()).acceptTrade(any());
        verify(dlqPublisher, never()).publishToDeadLetterQueue(any());
    }

    @Test
    void processTradeEvent_negative_errorPublishesToDlqAndRethrows() {
        TradeReceivedEvent event = sampleEvent();
        when(tradeMapper.eventToDomain(event)).thenThrow(new RuntimeException("mapping failed"));

        assertThrows(RuntimeException.class, () -> tradeProcessingService.processTradeEvent(event));

        verify(dlqPublisher).publishToDeadLetterQueue(event);
    }

    private TradeReceivedEvent sampleEvent() {
        return TradeReceivedEvent.builder()
                .eventId("E1")
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP1")
                .bookId("B1")
                .maturityDate(LocalDate.now().plusDays(5))
                .createdDate(LocalDate.now())
                .build();
    }

    private Trade sampleTrade() {
        return Trade.builder()
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP1")
                .bookId("B1")
                .maturityDate(LocalDate.now().plusDays(5))
                .createdDate(LocalDate.now())
                .build();
    }
}

