package com.stream.processing.alert.repository;

import com.stream.processing.alert.domain.AlertEntity;
import com.stream.processing.common.Severity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * Alert store access. The filtered listing goes through {@link JpaSpecificationExecutor} so the
 * optional criteria compose into one query instead of a combinatorial explosion of finder
 * methods; the summary aggregations are explicit JPQL because they must be computed by the
 * database, not by pulling rows into the JVM and counting them there.
 */
public interface AlertRepository extends JpaRepository<AlertEntity, String>,
        JpaSpecificationExecutor<AlertEntity> {

    /**
     * The ids from {@code candidates} that are already stored.
     *
     * <p>One round trip for a whole poll batch, which is what keeps the idempotent upsert cheap:
     * the alternative - {@code existsById} per record - would issue up to {@code max.poll.records}
     * queries for a batch that is usually entirely new.</p>
     */
    @Query("select a.alertId from AlertEntity a where a.alertId in :candidates")
    List<String> findExistingIds(@Param("candidates") Collection<String> candidates);

    /**
     * Alert counts per severity band inside a time range, for the dashboard's headline tiles.
     */
    @Query("""
            select a.severity as severity, count(a) as total
            from AlertEntity a
            where a.generatedAt >= :from and a.generatedAt < :to
            group by a.severity
            """)
    List<SeverityCount> countBySeverity(@Param("from") Instant from, @Param("to") Instant to);

    /**
     * Alert counts per district inside a time range, for the choropleth map.
     */
    @Query("""
            select a.districtId as districtId, count(a) as total
            from AlertEntity a
            where a.generatedAt >= :from and a.generatedAt < :to
            group by a.districtId
            """)
    List<DistrictCount> countByDistrict(@Param("from") Instant from, @Param("to") Instant to);

    @Query("select count(a) from AlertEntity a where a.generatedAt >= :from and a.generatedAt < :to")
    long countInRange(@Param("from") Instant from, @Param("to") Instant to);

    /** Projection for {@link #countBySeverity(Instant, Instant)}. */
    interface SeverityCount {
        Severity getSeverity();

        long getTotal();
    }

    /** Projection for {@link #countByDistrict(Instant, Instant)}. */
    interface DistrictCount {
        String getDistrictId();

        long getTotal();
    }
}
