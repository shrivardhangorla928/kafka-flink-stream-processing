package com.vassar.aware.alert.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vassar.aware.common.JsonCodec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.json.ProblemDetailJacksonMixin;

/**
 * Makes the shared pipeline codec the application's mapper.
 *
 * <p>The ingest service, the Flink job and this service must agree byte for byte on how an
 * {@code Instant} and an enum are written, or a topic becomes unreadable halfway along the chain.
 * {@link JsonCodec#create()} is the single definition of that format, so it is used here rather
 * than Spring Boot's independently-configured default mapper - which would, for instance, need
 * separate configuration to stop writing instants as epoch decimals.</p>
 */
@Configuration(proxyBeanMethods = false)
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = JsonCodec.create();

        // Spring's RFC 7807 support flattens a ProblemDetail's extension properties to the top
        // level through a Jackson mixin that Boot's own mapper builder installs. Bringing our own
        // mapper means bringing the mixin too, otherwise every error body nests the extensions
        // under a "properties" object and stops being valid problem+json.
        mapper.addMixIn(ProblemDetail.class, ProblemDetailJacksonMixin.class);
        return mapper;
    }
}
