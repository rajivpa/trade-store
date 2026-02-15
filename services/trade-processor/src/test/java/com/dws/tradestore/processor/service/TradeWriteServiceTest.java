package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.repository.TradeAuditRepository;
import com.dws.tradestore.processor.persistence.repository.TradeEntityRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeWriteServiceTest {

    @Mock
    private TradeEntityRepository tradeEntityRepository;

    @Mock
    private TradeAuditRepository tradeAuditRepository;

    @Mock
    private TradeStateStoreService tradeStateService;

    @Mock
    private TradeMapper tradeMapper;

    @InjectMocks
    private TradeWriteService tradeWriteService;

    @Test
    void acceptTrade_happyPath() {
        Trade trade = sampleTrade();
        TradeEntity tradeEntity = TradeEntity.builder().tradeId("T1").version(1).build();
        TradeAuditEntity tradeAuditEntity = TradeAuditEntity.builder().tradeId("T1").version(1).build();
        TradeState tradeState = TradeState.builder().tradeId("T1").lastProcessedVersion(1).status(TradeStatus.ACTIVE).build();

        when(tradeMapper.domainToSQLEntity(trade)).thenReturn(tradeEntity);
        when(tradeMapper.domainToNoSqlAuditEntity(trade)).thenReturn(tradeAuditEntity);
        when(tradeMapper.domainToStateEntity(trade)).thenReturn(tradeState);
        when(tradeEntityRepository.updateIfNewer(tradeEntity)).thenReturn(1);

        assertDoesNotThrow(() -> tradeWriteService.acceptTrade(trade));

        verify(tradeAuditRepository).save(tradeAuditEntity);
        verify(tradeStateService).publishUpdateEventForTradeStateStore(tradeState);
    }

    @Test
    void acceptTrade_negative_sqlWriteFailure() {
        Trade trade = sampleTrade();
        TradeEntity tradeEntity = TradeEntity.builder().tradeId("T1").version(1).build();
        TradeAuditEntity tradeAuditEntity = TradeAuditEntity.builder().tradeId("T1").version(1).build();
        TradeState tradeState = TradeState.builder().tradeId("T1").lastProcessedVersion(1).status(TradeStatus.ACTIVE).build();

        when(tradeMapper.domainToSQLEntity(trade)).thenReturn(tradeEntity);
        when(tradeMapper.domainToNoSqlAuditEntity(trade)).thenReturn(tradeAuditEntity);
        when(tradeMapper.domainToStateEntity(trade)).thenReturn(tradeState);
        when(tradeEntityRepository.updateIfNewer(tradeEntity)).thenReturn(0, 0);
        when(tradeEntityRepository.save(tradeEntity)).thenThrow(new RuntimeException("sql insert failed"));

        assertThrows(RuntimeException.class, () -> tradeWriteService.acceptTrade(trade));
    }

    @Test
    void expireTradeInNoSqlAndStateStore_happyPath() {
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();
        TradeAuditEntity auditEntity = TradeAuditEntity.builder().tradeId("T1").version(1).build();
        TradeState state = TradeState.builder().tradeId("T1").lastProcessedVersion(1).status(TradeStatus.EXPIRED).build();

        when(tradeMapper.expiryEventToAuditEntity(event)).thenReturn(auditEntity);
        when(tradeMapper.expiryEventToStateEntity(event)).thenReturn(state);

        assertDoesNotThrow(() -> tradeWriteService.expireTradeInNoSqlAndStateStore(event));

        verify(tradeAuditRepository).save(auditEntity);
        verify(tradeStateService).publishUpdateEventForTradeStateStore(state);
    }

    @Test
    void expireTradeInNoSqlAndStateStore_negative_mapperFailure() {
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();
        when(tradeMapper.expiryEventToAuditEntity(event)).thenThrow(new RuntimeException("mapping failed"));

        assertThrows(RuntimeException.class, () -> tradeWriteService.expireTradeInNoSqlAndStateStore(event));
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
