package com.dws.tradestore.processor.event.inbound;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TickEvent {
    private String tickId;
    private LocalDate businessDate;
}
