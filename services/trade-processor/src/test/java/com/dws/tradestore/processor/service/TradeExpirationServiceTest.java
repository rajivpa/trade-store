package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.messaging.publisher.TradeExpiryPublisher;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.repository.TradeEntityRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import java.util.List;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeExpirationServiceTest {

    @Mock
    private TradeEntityRepository tradeEntityRepository;

    @Mock
    private TradeMapper tradeMapper;

    @Mock
    private TradeExpiryPublisher tradeExpiryPublisher;

    @InjectMocks
    private TradeExpirationService tradeExpirationService;

    @Test
    void expireTradeAsOf_happyPath_noTradesFound() {
        when(tradeEntityRepository.findExpiredTradesPageable(any(), any())).thenReturn(Page.empty());

        assertDoesNotThrow(() -> tradeExpirationService.expireTradeAsOf(LocalDate.now()));

        verify(tradeEntityRepository).findExpiredTradesPageable(any(), any());
        verify(tradeEntityRepository, never()).bulkExpireTradesById(any());
        verify(tradeExpiryPublisher, never()).publishTradeExpiredEvent(any());
    }

    @Test
    void expireTradeAsOf_negative_nullDate() {
        assertThrows(RuntimeException.class, () -> tradeExpirationService.expireTradeAsOf(null));
    }

    @Test
    void expireTradeAsOf_negative_zeroRowsUpdated_stopsLoopAndSkipsPublish() {
        TradeEntity entity = TradeEntity.builder().tradeId("T1").build();
        when(tradeEntityRepository.findExpiredTradesPageable(any(), any()))
                .thenReturn(new PageImpl<>(List.of(entity)));
        when(tradeEntityRepository.bulkExpireTradesById(any())).thenReturn(0);

        assertDoesNotThrow(() -> tradeExpirationService.expireTradeAsOf(LocalDate.now()));

        verify(tradeEntityRepository).findExpiredTradesPageable(any(), any());
        verify(tradeEntityRepository).bulkExpireTradesById(any());
        verify(tradeExpiryPublisher, never()).publishTradeExpiredEvent(any());
    }

}
