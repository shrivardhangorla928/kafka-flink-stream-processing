package com.vassar.aware.alert.web.dto;

import org.springframework.data.domain.Page;

import java.util.List;
import java.util.function.Function;

/**
 * Stable envelope for paginated responses.
 *
 * <p>Spring Data's {@code PageImpl} serialises to a shape its own documentation warns is not a
 * stable contract, and it exposes the internal {@code Pageable}/{@code Sort} structures. This
 * record pins the JSON the dashboard depends on to five fields that will not move.</p>
 *
 * @param content       the page's rows
 * @param page          zero-based page number
 * @param size          requested page size, after the server-side cap has been applied
 * @param totalElements matching rows across all pages
 * @param totalPages    number of pages at this size
 */
public record PageResponse<T>(List<T> content,
                              int page,
                              int size,
                              long totalElements,
                              int totalPages) {

    public static <E, T> PageResponse<T> of(Page<E> page, Function<E, T> mapper) {
        return new PageResponse<>(
                page.getContent().stream().map(mapper).toList(),
                page.getNumber(),
                page.getSize(),
                page.getTotalElements(),
                page.getTotalPages());
    }
}
