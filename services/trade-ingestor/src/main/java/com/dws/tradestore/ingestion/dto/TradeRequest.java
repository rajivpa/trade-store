package com.dws.tradestore.ingestion.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeRequest {

    @NotBlank(message = "Trade Id is mandatory")
    private String tradeId;

    @NotNull(message = "Version is mandatory")
    private Integer version;

    @NotBlank(message = "Counter-Party Id is mandatory")
    private String counterPartyId;

    @NotBlank(message = "Book-Id is mandatory")
    private String bookId;

    @NotBlank(message = "Maturity Date is mandatory")
    private String maturityDate;

    private LocalDate createdDate;

    private Map<String, String> headers;
}
