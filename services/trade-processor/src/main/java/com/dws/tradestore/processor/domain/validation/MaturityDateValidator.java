package com.dws.tradestore.processor.domain.validation;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

@Slf4j
@Component
public class MaturityDateValidator  extends BaseValidator {

    protected ValidationResult doValidate(Trade trade, Optional<TradeState> currentTradeState) {
        if(trade.getMaturityDate().isBefore(LocalDate.now())){
            log.warn("Maturity date is in past for incoming trade Id {}, version {}", trade.getTradeId(), trade.getVersion());
            ValidationResult result = ValidationResult.failure("Invalid maturity date");
            return result;
        }
        return ValidationResult.success();
    }
}
