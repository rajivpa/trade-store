package com.dws.tradestore.processor.domain.validation;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class VersionValidator extends BaseValidator {


    @Override
    protected ValidationResult doValidate(Trade incomingTrade, Optional<TradeState> currentTradeState) {

        if(currentTradeState.isEmpty()){
            log.info("Trade Id : {}, Version: {} is new. Version check passed", incomingTrade.getTradeId(), incomingTrade.getVersion());
            return ValidationResult.success();
        }

        if(incomingTrade.getVersion() < currentTradeState.get().getLastProcessedVersion()){
            String reason = String.format("Trade %s rejected: incoming version %d is lower than current version %d",
                    incomingTrade.getTradeId(), incomingTrade.getVersion(), currentTradeState.get().getLastProcessedVersion());
            log.warn(reason);
            return ValidationResult.failure(reason);
        }

        if(incomingTrade.getVersion().equals(currentTradeState.get().getLastProcessedVersion())){
            log.info("Trade Id : {} received with same version: {} is new. Will be replacing existing trade",
                    incomingTrade.getTradeId(), incomingTrade.getVersion());
        } else {
            log.info("Trade Id : {} received with higher version: {}. Will update the version on trade",
                    incomingTrade.getTradeId(), incomingTrade.getVersion());
        }
        return ValidationResult.success();
    }
}
