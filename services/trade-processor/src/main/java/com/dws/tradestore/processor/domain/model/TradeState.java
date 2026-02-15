package com.dws.tradestore.processor.domain.model;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
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
public class TradeState {

    private String tradeId;
    private Integer lastProcessedVersion;
    private TradeStatus status;
}
