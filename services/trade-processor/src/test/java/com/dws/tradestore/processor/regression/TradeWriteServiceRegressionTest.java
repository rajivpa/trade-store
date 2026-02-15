package com.dws.tradestore.processor.regression;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.mapper.TradeMapper;
import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.repository.TradeAuditRepository;
import com.dws.tradestore.processor.persistence.repository.TradeEntityRepository;
import com.dws.tradestore.processor.service.TradeStateStoreService;
import com.dws.tradestore.processor.service.TradeWriteService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeWriteServiceRegressionTest {

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
    void regression_noSqlSaveShouldStopAfterFirstSuccess() {
        Trade trade = Trade.builder()
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP1")
                .bookId("B1")
                .maturityDate(LocalDate.now().plusDays(5))
                .createdDate(LocalDate.now())
                .build();

        TradeEntity tradeEntity = TradeEntity.builder().tradeId("T1").version(1).build();
        TradeAuditEntity auditEntity = TradeAuditEntity.builder().tradeId("T1").version(1).build();
        TradeState state = TradeState.builder().tradeId("T1").lastProcessedVersion(1).status(TradeStatus.ACTIVE).build();

        when(tradeMapper.domainToSQLEntity(trade)).thenReturn(tradeEntity);
        when(tradeMapper.domainToNoSqlAuditEntity(trade)).thenReturn(auditEntity);
        when(tradeMapper.domainToStateEntity(trade)).thenReturn(state);
        when(tradeEntityRepository.updateIfNewer(tradeEntity)).thenReturn(1);

        tradeWriteService.acceptTrade(trade);

        verify(tradeAuditRepository).save(auditEntity);
    }
}

