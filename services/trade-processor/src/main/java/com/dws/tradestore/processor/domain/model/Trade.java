package com.dws.tradestore.processor.domain.model;

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
public class Trade {

    private String tradeId;
    private Integer version;
    private String counterPartyId;
    private String bookId;
    private LocalDate maturityDate;
    private LocalDate createdDate;

    private TransmissionChannel transmissionChannel;
    private LocalDateTime ingestionTimestamp;
    private LocalDateTime processingTimestamp;

    public static Trade createNew(String tradeId, Integer version) {
        return Trade.builder()
                .tradeId(tradeId)
                .version(version)
                .createdDate(LocalDate.now())
                .ingestionTimestamp(LocalDateTime.now())
                .build();
    }
}
