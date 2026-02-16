package com.dws.tradestore.ingestion.adapter;

import com.dws.tradestore.ingestion.dto.TradeRequest;
import com.dws.tradestore.ingestion.event.enums.TransmissionChannel;

public interface ITradeIngestor {

    /**
     * ingests the trade into TradeStore platform by sending it to streaming service
     * @param request
     */
    void processTradeRequest(TradeRequest request);

    /**
     * returns the transmission channel through which trade was received
     * @return
     */
    TransmissionChannel getSourceChannel();

}
