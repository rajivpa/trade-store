package com.dws.tradestore.processor.domain.validation;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public abstract class BaseValidator implements TradeValidator{

    protected TradeValidator next;

    @Override
    public TradeValidator setNext(TradeValidator next){
        this.next = next;
        return next;
    }

    public ValidationResult validate(Trade trade, Optional<TradeState> currentTradeState ){

        ValidationResult result = doValidate(trade,currentTradeState);

        if(!result.isValid()){
            log.warn("Validation failed in {}: {}", this.getClass().getSimpleName(), result.getErrors());
            return result;
        }

        if(next != null){
            return next.validate(trade, currentTradeState);
        }

        return result;
    }

    protected abstract ValidationResult doValidate(Trade trade, Optional<TradeState> currentTradeState);

}
