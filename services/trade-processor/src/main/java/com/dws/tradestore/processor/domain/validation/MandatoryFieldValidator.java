package com.dws.tradestore.processor.domain.validation;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class MandatoryFieldValidator extends BaseValidator {

    private static final String MISSING_TRADE_ID = "Missing TradeId";
    private static final String MISSING_VERSION = "Missing version";
    private static final String MISSING_COUNTER_PARTY_ID = "Missing Counter-Party Id";
    private static final String INVALID_VERSION = "Invalid version";
    private static final String MISSING_BOOK_ID = "Missing book id value";
    private static final String MISSING_MATURITY_DATE = "Missing maturity date";
    private static final String MISSING_CREATED_DATE = "Missing created date";

    @Override
    protected ValidationResult doValidate(Trade trade, Optional<TradeState> currentTradeState) {

        ValidationResult result = ValidationResult.success();

        if(trade.getTradeId() == null || trade.getTradeId().isBlank()){
            log.warn("Missing Trade Id in the incoming trade");
            result.addError(MISSING_TRADE_ID);
        }

        if(trade.getVersion() == null){
            log.warn("Missing version in the incoming trade with Trade Id : {}",trade.getTradeId());
            result.addError(MISSING_VERSION);
        } else if (trade.getVersion() < 0){
            log.warn("Invalid version in the incoming trade with Trade Id : {}",trade.getTradeId());
            result.addError(INVALID_VERSION);
        }

        if(trade.getCounterPartyId() == null || trade.getCounterPartyId().isBlank()){
            log.warn("Missing counter-party-id in the incoming trade with Trade Id : {}",trade.getTradeId());
            result.addError(MISSING_COUNTER_PARTY_ID);
        }

        if(trade.getBookId() == null || trade.getBookId().isBlank()){
            log.warn("Missing bookId in the incoming trade with Trade Id : {}",trade.getTradeId());
            result.addError(MISSING_BOOK_ID);
        }

        if(trade.getMaturityDate() == null){
            log.warn("Missing maturity date in the incoming trade with Trade Id : {}",trade.getTradeId());
            result.addError(MISSING_MATURITY_DATE);
        }

        if(trade.getCreatedDate() == null){
            log.warn("Missing created date in the incoming trade with Trade Id : {}",trade.getTradeId());
            result.addError(MISSING_CREATED_DATE);
        }

        return result;
    }
}
