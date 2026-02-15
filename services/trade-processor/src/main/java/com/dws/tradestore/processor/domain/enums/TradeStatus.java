package com.dws.tradestore.processor.domain.enums;

import lombok.Getter;

@Getter
public enum TradeStatus {
    ACTIVE("ACTIVE"),
    REJECTED("REJECTED"),
    EXPIRED("EXPIRED");

    private final String displayName;

    TradeStatus(String displayName) {
        this.displayName = displayName;
    }
}
