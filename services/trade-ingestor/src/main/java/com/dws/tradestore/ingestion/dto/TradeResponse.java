package com.dws.tradestore.ingestion.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeResponse {
    private String tradeId;
    private Integer version;
    private String status;
    private String message;
    private LocalDateTime timestamp;
    private String idempotencyKey;

    /**
     * Create accepted response
     * @param tradeId
     * @param version
     * @return
     */
    public static TradeResponse success(String tradeId, Integer version){
        return TradeResponse.builder()
                .tradeId(tradeId)
                .version(version)
                .status("ACCEPTED")
                .message("Trade Accepted")
                .timestamp(LocalDateTime.now())
                .build();
    }

    /**
     * Create rejection response
     * @param tradeId
     * @param reason
     * @return
     */
    public static TradeResponse rejected(String tradeId, String reason){
        return TradeResponse.builder()
                .tradeId(tradeId)
                .status("REJECTED")
                .message(reason)
                .timestamp(LocalDateTime.now())
                .build();
    }

    /**
     * Create error response
     * @param errMsg
     * @return
     */
    public static TradeResponse error(String errMsg){
        return TradeResponse.builder()
                .status("ERROR")
                .message(errMsg)
                .timestamp(LocalDateTime.now())
                .build();
    }
}
