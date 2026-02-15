package com.dws.tradestore.processor.event.outbound;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeRejectedEvent {

    private String rejectionId;
    private String tradeId;
    private Integer version;

    private String stage;
    private String rejectionReason;
    private List<String> failedValidations;
    private String errorDetails;

    private String sourceChannel;
    private LocalDateTime ingestionTimestamp;
    private LocalDateTime rejectionTimestamp;
    private Integer currentVersion;
}
