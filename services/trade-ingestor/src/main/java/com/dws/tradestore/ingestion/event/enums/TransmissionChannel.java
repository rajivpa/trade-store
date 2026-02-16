package com.dws.tradestore.ingestion.event.enums;

import lombok.Getter;

@Getter
public enum TransmissionChannel {

    REST_API("REST_API"),
    MQ("MSG_QUEUE"),
    FILE_UPLOAD("FILE_UPLOAD"),
    UNKNOWN("UNKNOWN");

    private final String displayName;

    TransmissionChannel(String displayName){
        this.displayName = displayName;
    }
}
