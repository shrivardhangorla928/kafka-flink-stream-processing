package com.stream.processing.alert.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /api/v1/alerts/{alertId}/acknowledge}.
 *
 * <p>{@code acknowledgedBy} is mandatory: an acknowledgement with no name attached is worthless
 * as an audit trail, which is the only reason the field is stored at all.</p>
 */
public record AcknowledgeRequest(@NotBlank @Size(max = 128) String acknowledgedBy) {
}
