package com.dws.tradestore.ingestion.exception;

public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message){
        super(message);
    }

    public KafkaPublishException(String message, Throwable th){
        super(message, th);
    }
}
