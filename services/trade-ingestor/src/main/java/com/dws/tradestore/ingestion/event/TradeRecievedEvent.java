package com.dws.tradestore.ingestion.event;

import java.time.LocalDate;
import java.time.LocalDateTime;

import com.dws.tradestore.ingestion.event.enums.TransmissionChannel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeRecievedEvent {

    private String tradeId;
    private Integer version;
    private String counterPartyId;
    private String bookId;
    private LocalDate maturityDate;
    private LocalDate createdDate;
    private char expired;
    private TransmissionChannel transmissionChannel;
    private LocalDateTime ingestionTimestamp;
    private LocalDateTime processingTimestamp;

    public static TradeRecievedEvent createNew() {
        return TradeRecievedEvent.builder()
                .ingestionTimestamp(LocalDateTime.now())
                .expired('N')
                .build();
    }

    public boolean isExpired() {
        return maturityDate != null && maturityDate.isBefore(LocalDate.now());
    }
}
