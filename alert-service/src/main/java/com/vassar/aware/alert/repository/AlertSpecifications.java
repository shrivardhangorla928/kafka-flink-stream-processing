package com.vassar.aware.alert.repository;

import com.vassar.aware.alert.domain.AlertEntity;
import jakarta.persistence.criteria.Predicate;
import org.springframework.data.jpa.domain.Specification;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns an {@link AlertFilter} into a single {@code WHERE} clause.
 *
 * <p>Absent criteria contribute no predicate at all, rather than a {@code 1=1} placeholder, so
 * the planner sees exactly the columns that were asked for and can pick the matching composite
 * index from {@code V1__alerts.sql}.</p>
 */
public final class AlertSpecifications {

    private AlertSpecifications() {
        // static holder
    }

    public static Specification<AlertEntity> matching(AlertFilter filter) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (filter.districtId() != null) {
                predicates.add(cb.equal(root.get("districtId"), filter.districtId()));
            }
            if (filter.stationId() != null) {
                predicates.add(cb.equal(root.get("stationId"), filter.stationId()));
            }
            if (filter.sensorType() != null) {
                predicates.add(cb.equal(root.get("sensorType"), filter.sensorType()));
            }
            if (filter.minSeverity() != null) {
                // The denormalised rank is what makes "at least this severe" an index range scan;
                // comparing the enum itself would order it alphabetically, which is meaningless.
                predicates.add(cb.greaterThanOrEqualTo(
                        root.get("severityRank"), filter.minSeverity().rank()));
            }
            if (filter.from() != null) {
                predicates.add(cb.greaterThanOrEqualTo(root.get("generatedAt"), filter.from()));
            }
            if (filter.to() != null) {
                // Exclusive: back-to-back ranges must not double-count an alert on the boundary.
                predicates.add(cb.lessThan(root.get("generatedAt"), filter.to()));
            }
            if (filter.acknowledged() != null) {
                predicates.add(cb.equal(root.get("acknowledged"), filter.acknowledged()));
            }

            return predicates.isEmpty()
                    ? cb.conjunction()
                    : cb.and(predicates.toArray(new Predicate[0]));
        };
    }
}
