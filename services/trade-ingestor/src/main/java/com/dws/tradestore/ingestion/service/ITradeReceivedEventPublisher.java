package com.dws.tradestore.ingestion.service;

import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import com.dws.tradestore.ingestion.event.TradeRecievedEvent;

public interface ITradeReceivedEventPublisher {

    void publishTradeReceivedEvent(TradeRecievedEvent trade);
    void publishIngestionRejectedEvent(RejectedTradeEvent rejectedTrade);
    //void publishProcessingRejectedTrade(RejectedTradeEvent rejectedTrade);
}
