package com.dws.tradestore.ingestion.controller;

import com.dws.tradestore.ingestion.adapter.rest.RestApiTradeAdapter;
import com.dws.tradestore.ingestion.dto.TradeRequest;
import com.dws.tradestore.ingestion.dto.TradeResponse;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("${trade.ingestion.api.base-path:/api/v1/trades}")
@Validated
public class TradeIngestionController {

    @Autowired
    private RestApiTradeAdapter restTradeAdapter;

    /**
     * Recieve the Trade request and forward it for processing & publishing to streaming service
     * @param request
     * @return
     */
    @PostMapping
    public ResponseEntity<TradeResponse> processTradeRequest(@Valid @RequestBody TradeRequest request){
        log.info("Received trade request with Trade Id {}, Version: {}", request.getTradeId(),request.getVersion());
        try{
            restTradeAdapter.processTradeRequest(request);
            TradeResponse response = TradeResponse.success(request.getTradeId(),request.getVersion());
            log.info("Accepted trade request with Trade Id {}, Version {}",request.getTradeId(),request.getVersion());
            return ResponseEntity.accepted().body(response);
        } catch (IllegalArgumentException iae){
            log.error("Invalid trade request: {}", iae.getMessage());
            TradeResponse response = TradeResponse.rejected(request.getTradeId(), iae.getMessage());
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e){
            log.error("Error processing trade: {}", e.getMessage(),e);
            TradeResponse response = TradeResponse.error("Error processing request "+e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}
