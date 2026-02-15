package com.dws.tradestore.processor.domain.validation;

import com.dws.tradestore.processor.domain.enums.TradeStatus;
import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Optional;

@Slf4j
@Component
public class ExpiredStatusValidator extends BaseValidator{

    protected ValidationResult doValidate(Trade trade, Optional<TradeState> currentTradeState) {
        if(!currentTradeState.isEmpty() && currentTradeState.get().getStatus() == TradeStatus.EXPIRED){
            log.warn("The trade for incoming event has already expired. trade Id {}, version {}", trade.getTradeId(), trade.getVersion());
            ValidationResult result = ValidationResult.failure("Trade has expired, no updates allowed");
            return result;
        }
        return ValidationResult.success();
    }
}
