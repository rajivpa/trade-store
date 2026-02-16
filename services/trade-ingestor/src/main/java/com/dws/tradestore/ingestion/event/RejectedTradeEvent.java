package com.dws.tradestore.ingestion.event;

import com.dws.tradestore.ingestion.event.enums.RejectionStage;
import com.dws.tradestore.ingestion.event.enums.TransmissionChannel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RejectedTradeEvent {
    private String tradeId;
    private Integer version;
    private String rejectionReason;
    private RejectionStage stage;
    private LocalDateTime ingestionTimestamp;
    private LocalDateTime rejectionTimestamp;
    private TransmissionChannel sourceChannel;
    private Object rawPayload;
}
