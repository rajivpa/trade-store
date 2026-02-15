package com.dws.tradestore.processor.domain.validation;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;

import java.util.Optional;

public interface TradeValidator {
    ValidationResult validate(Trade trade, Optional<TradeState> currentTradeState);
    TradeValidator setNext(TradeValidator next);
}
