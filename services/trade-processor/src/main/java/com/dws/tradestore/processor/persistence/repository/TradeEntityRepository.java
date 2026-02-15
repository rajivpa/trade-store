package com.dws.tradestore.processor.persistence.repository;

import com.dws.tradestore.processor.persistence.entity.TradeEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface TradeEntityRepository extends JpaRepository<TradeEntity, String> {

    @Modifying
    @Query("""
        update TradeEntity t
            set t.version = :#{#entity.version},
                t.counterPartyId = :#{#entity.counterPartyId},
                t.bookId = :#{#entity.bookId},
                t.maturityDate = :#{#entity.maturityDate},
                t.updatedAt = :#{#entity.updatedAt}
                where t.tradeId = :#{#entity.tradeId}
                and t.version <= :#{#entity.version}
     """)
    int updateIfNewer(@Param("entity") TradeEntity entity);

    /**
     * Find all non-expired trades with maturity date less than given date. Filters
     * already expired trades ( marked by expired="Y" )
     * @param asOfDate
     * @return
     */
    @Query("""
        SELECT t from TradeEntity t
        WHERE t.expired != 'Y'
        AND CAST(t.maturityDate AS date) < :asOfDate    
    """)
    Page<TradeEntity> findExpiredTradesPageable(@Param("asOfDate") LocalDate asOfDate, Pageable pageable);

    @Modifying
    @Transactional
    @Query("""
        UPDATE TradeEntity t SET
            t.expired = 'Y',
            t.updatedAt = CURRENT_TIMESTAMP
        WHERE t.tradeId IN :tradeIds
    """)
    int bulkExpireTradesById(@Param("tradeIds")List<String> tradeIds);


}
