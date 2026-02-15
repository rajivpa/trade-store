package com.dws.tradestore.processor.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(
        name = "trades",
        indexes = {
                @Index(name = "idx_trade_id", columnList = "trade_id"),
                @Index(name = "idx_maturity_date", columnList = "maturity_date"),
        }
)

public class TradeEntity {

    @Id
    @Column(name = "trade_id", nullable = false, length = 50)
    private String tradeId;

    @Column(name = "version", nullable = false)
    private Integer version;

    @Column(name = "counter_party_id", nullable = false, length = 50)
    private String counterPartyId;

    @Column(name = "book_id", nullable = false, length = 50)
    private String bookId;

    @Column(name = "maturity_date", nullable = false)
    private LocalDate maturityDate;

    @Column(name = "created_date", nullable = false)
    private LocalDate createdDate;

    @Column(name = "expired", length = 1)
    private String expired; // "Y" or "N"

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        if (expired == null || expired.isBlank()) {
            expired = "N";
        }
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
