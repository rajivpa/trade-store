package com.dws.tradestore.processor.mapper;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.event.inbound.TradeReceivedEvent;
import com.dws.tradestore.processor.event.outbound.TradeExpiredEvent;
import com.dws.tradestore.processor.messaging.publisher.TradeExpiryPublisher;
import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import com.dws.tradestore.processor.persistence.entity.TradeAuditEntity;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class TradeMapper {

    /**
     * Convert Event → Domain
     * This is where we enter the business domain
     */
    public Trade eventToDomain(TradeReceivedEvent event) {
        return Trade.builder()
                .tradeId(event.getTradeId())
                .version(event.getVersion())
                .counterPartyId(event.getCounterPartyId())
                .bookId(event.getBookId())
                .maturityDate(event.getMaturityDate())
                .createdDate(event.getCreatedDate())
                .transmissionChannel(event.getTransmissionChannel())
                .ingestionTimestamp(event.getReceivedTimestamp())
                .processingTimestamp(LocalDateTime.now())
                .build();
    }

    public TradeEntity domainToSQLEntity(Trade trade) {
        return TradeEntity.builder()
                .tradeId(trade.getTradeId())
                .version(trade.getVersion())
                .counterPartyId(trade.getCounterPartyId())
                .bookId(trade.getBookId())
                .maturityDate(trade.getMaturityDate())
                .createdDate(trade.getCreatedDate())
                .expired("N")
                .build();
    }

    public TradeAuditEntity domainToNoSqlAuditEntity(
            Trade trade)
         {
        // NOTE: UUID eventId makes each write attempt unique; retries can create duplicate logical audit entries.
        // Future enhancement: deterministic idempotency key (tradeId+version+eventType) for deduplication.
        return TradeAuditEntity.builder()
                .tradeId(trade.getTradeId())
                .eventTimestamp(LocalDateTime.now())
                .eventId(UUID.randomUUID())
                .version(trade.getVersion())
                .counterPartyId(trade.getCounterPartyId())
                .bookId(trade.getBookId())
                .maturityDate(trade.getMaturityDate())
                .createdDate(trade.getCreatedDate())
                .expired("N")
                .status(TradeStatus.ACTIVE.name())
                .transmissionChannel(trade.getTransmissionChannel().name())
                .ingestionTimestamp(trade.getIngestionTimestamp())
                .processingTimestamp(trade.getProcessingTimestamp())
                .build();
    }

    public TradeState domainToStateEntity(Trade trade){
        return TradeState.builder()
                .tradeId(trade.getTradeId())
                .lastProcessedVersion(trade.getVersion())
                .status(TradeStatus.ACTIVE).build();
    }

    public TradeExpiredEvent tradeEntityToExpiredEvent(TradeEntity trade){
        return TradeExpiredEvent.builder()
                .tradeId(trade.getTradeId())
                .version(trade.getVersion())
                .bookId(trade.getBookId())
                .counterPartyId(trade.getCounterPartyId())
                .maturityDate(trade.getMaturityDate())
                .createdDate(trade.getCreatedDate())
                .expired("Y")
                .status(TradeStatus.EXPIRED.name()).build();
    }

    public TradeAuditEntity expiryEventToAuditEntity(TradeExpiredEvent tradeExpiredEvent){
        // NOTE: UUID eventId makes each write attempt unique; retries can create duplicate logical audit entries.
        // Future enhancement: deterministic idempotency key (tradeId+version+eventType) for deduplication.
        return TradeAuditEntity.builder()
                .tradeId(tradeExpiredEvent.getTradeId())
                .eventTimestamp(LocalDateTime.now())
                .eventId(UUID.randomUUID())
                .version(tradeExpiredEvent.getVersion())
                .counterPartyId(tradeExpiredEvent.getCounterPartyId())
                .bookId(tradeExpiredEvent.getBookId())
                .maturityDate(tradeExpiredEvent.getMaturityDate())
                .createdDate(tradeExpiredEvent.getCreatedDate())
                .expired(tradeExpiredEvent.getExpired())
                .status(tradeExpiredEvent.getStatus())
                .build();
    }

    public TradeState expiryEventToStateEntity(TradeExpiredEvent tradeExpiredEvent){
        return TradeState.builder()
                .tradeId(tradeExpiredEvent.getTradeId())
                .lastProcessedVersion(tradeExpiredEvent.getVersion())
                .status(TradeStatus.EXPIRED).build();
    }
}
