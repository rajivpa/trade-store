package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import com.dws.tradestore.processor.domain.validation.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class ValidationService {

    private final BaseValidator validationChain;

    @Autowired
    public ValidationService(
            MandatoryFieldValidator mandatoryFieldsValidator,
            VersionValidator versionValidator, MaturityDateValidator maturityDateValidator,
            ExpiredStatusValidator expiredStatusValidator) {

        log.info("Building validation chain");

        // Build chain: Mandatory → Version → Maturity → ExpiredStatus
        this.validationChain = mandatoryFieldsValidator;
        mandatoryFieldsValidator.setNext(versionValidator);
        versionValidator.setNext(maturityDateValidator);
        maturityDateValidator.setNext(expiredStatusValidator);

        log.info("Validation chain built successfully with {} validators", 4);
    }

    public ValidationResult validate(Trade incomingTrade, Optional<TradeState> currenTradeState){
        log.debug("Starting business validation chain for trade: with Trade Id {} & version : {}",
                incomingTrade.getTradeId(), incomingTrade.getVersion());

        ValidationResult result;

        try {
            // Execute validation chain
            result = validationChain.validate(incomingTrade, currenTradeState);

            if (!result.isValid()) {
                log.warn("Trade validation failed: for Trade Id {} & version : {}. Errors: {}",
                        incomingTrade.getTradeId(), incomingTrade.getVersion(), result.getErrors());
            } else {
                log.info("Trade validation passed: for Trade Id {} & version {}",
                        incomingTrade.getTradeId(), incomingTrade.getVersion());
            }

        } catch (Exception e) {
            log.error("Error during validation chain execution for trade: {}-{}. Error: {}",
                    incomingTrade.getTradeId(), incomingTrade.getVersion(), e.getMessage(), e);
            result = ValidationResult.failure("Error while validating incoming trade");
        }

        return result;
    }
}
