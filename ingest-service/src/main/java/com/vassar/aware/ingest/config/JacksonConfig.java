package com.vassar.aware.ingest.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vassar.aware.common.JsonCodec;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Makes the shared pipeline codec the application's one and only {@link ObjectMapper}.
 *
 * <p>Without this, the REST layer would use Boot's auto-configured mapper while the Kafka
 * serializer used {@link JsonCodec}, and the two would eventually disagree about instants -
 * a reading accepted over HTTP could then be written to the topic in a shape the Flink stage
 * cannot read. Declaring the bean also makes {@link JacksonAutoConfiguration} back off.</p>
 */
@Configuration(proxyBeanMethods = false)
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonCodec.create();
    }
}
