package com.dws.tradestore.ingestion.adapter.rest;

import com.dws.tradestore.ingestion.adapter.BaseTradeIngestor;
import com.dws.tradestore.ingestion.dto.TradeRequest;
import com.dws.tradestore.ingestion.event.TradeRecievedEvent;
import com.dws.tradestore.ingestion.validation.ValidationResult;
import com.dws.tradestore.ingestion.event.enums.TransmissionChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
@Component
public class RestApiTradeAdapter extends BaseTradeIngestor {

    @Value("${trade.ingestion.validation.maturity-date-format:yyyy-MM-dd}")
    private String maturityDateFormat = "yyyy-MM-dd";

    @Override
    protected ValidationResult validateTrade(TradeRequest request) {
        ValidationResult result = new ValidationResult();

        if( !StringUtils.hasText(request.getTradeId())){
            result.addError("Missing trade id");
        }
        if( request.getVersion()==null || request.getVersion() < 0){
            result.addError("Missing or Invalid version id");
        }
        if( !StringUtils.hasText(request.getCounterPartyId())){
            result.addError("Missing Counter Party id");
        }
        if( !StringUtils.hasText(request.getBookId())){
            result.addError("Missing book id");
        }
        if( !StringUtils.hasText(request.getMaturityDate())){
            result.addError("Missing maturity date");
        } else {
            try {
                LocalDate.parse(request.getMaturityDate(), DateTimeFormatter.ofPattern(maturityDateFormat));
            }catch (DateTimeParseException dtpe){
                result.addError("Invalid maturity date");
            }
        }
        return result;
    }

    @Override
    protected TradeRecievedEvent transformTrade(TradeRequest request) {

        return TradeRecievedEvent.builder()
                .tradeId(request.getTradeId())
                .version(request.getVersion())
                .counterPartyId(request.getCounterPartyId())
                .bookId(request.getBookId())
                .maturityDate(LocalDate.parse(request.getMaturityDate(), DateTimeFormatter.ofPattern(maturityDateFormat)))
                .createdDate(LocalDate.now())
                .expired('N')
                .transmissionChannel(TransmissionChannel.REST_API)
                .ingestionTimestamp(LocalDateTime.now()).build();
    }

    @Override
    public TransmissionChannel getSourceChannel() {
        return TransmissionChannel.REST_API;
    }
}
