package com.dws.tradestore.processor.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("trade_history")
public class TradeAuditEntity {

    @PrimaryKeyColumn(name = "trade_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String tradeId;

    @PrimaryKeyColumn(
            name = "event_timestamp",
            ordinal = 1,
            type = PrimaryKeyType.CLUSTERED,
            ordering = Ordering.DESCENDING
    )
    private LocalDateTime eventTimestamp;

    @PrimaryKeyColumn(name = "event_id", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
    private UUID eventId;

    @Column("version")
    private Integer version;

    @Column("counter_party_id")
    private String counterPartyId;

    @Column("book_id")
    private String bookId;

    @Column("maturity_date")
    private LocalDate maturityDate;

    @Column("created_date")
    private LocalDate createdDate;

    @Column("expired")
    private String expired;

    @Column("status")
    private String status;

    @Column("transmission_channel")
    private String transmissionChannel;

    // ========== Event Metadata ==========
    @Column("ingestion_timestamp")
    private LocalDateTime ingestionTimestamp;

    @Column("processing_timestamp")
    private LocalDateTime processingTimestamp;
}
