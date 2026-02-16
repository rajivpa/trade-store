package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TradeExpiredEventProcessingServiceTest {

    @Mock
    private TradeWriteService tradeWriteService;

    @InjectMocks
    private TradeExpiredEventProcessingService tradeExpiredEventProcessingService;

    @Test
    void processExpiredEventForAuditAndStateStore_happyPath() {
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();

        assertDoesNotThrow(() -> tradeExpiredEventProcessingService.processExpiredEventForAuditAndStateStore(event));

        verify(tradeWriteService).expireTradeInNoSqlAndStateStore(event);
    }

    @Test
    void processExpiredEventForAuditAndStateStore_negative_downstreamFailure() {
        TradeExpiredEvent event = TradeExpiredEvent.builder().tradeId("T1").version(1).build();
        doThrow(new RuntimeException("write failed"))
                .when(tradeWriteService)
                .expireTradeInNoSqlAndStateStore(event);

        assertThrows(
                RuntimeException.class,
                () -> tradeExpiredEventProcessingService.processExpiredEventForAuditAndStateStore(event)
        );
    }
}
