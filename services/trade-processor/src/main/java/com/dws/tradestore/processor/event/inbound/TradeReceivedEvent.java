package com.dws.tradestore.processor.event.inbound;

import com.dws.tradestore.processor.event.enums.TransmissionChannel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeReceivedEvent {
    private String eventId;
    private TransmissionChannel transmissionChannel;
    private LocalDateTime receivedTimestamp;
    private int schemaVersion;

    private String tradeId;
    private Integer version;
    private String counterPartyId;
    private String bookId;
    private LocalDate maturityDate;
    private LocalDate createdDate;

    public static TradeReceivedEvent createNew() {
        return TradeReceivedEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .receivedTimestamp(LocalDateTime.now())
                .build();
    }

    public String getIdempotencyKey() {
        return String.format("%s:%s", tradeId, version);
    }
}
