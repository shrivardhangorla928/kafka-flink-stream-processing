package com.stream.processing.alert;

import com.stream.processing.alert.repository.AlertRepository;
import com.stream.processing.alert.repository.StationWindowAggregateRepository;
import com.stream.processing.common.Topics;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Shared full-context setup for the integration tests: the whole application, an in-memory H2
 * database carrying the real Flyway schema, and an in-process Kafka broker.
 *
 * <p>Neither PostgreSQL nor Kafka needs to be running. That is a hard requirement, not a
 * convenience - these tests are the SonarQube coverage gate's input and run on every CI build,
 * where no external service exists.</p>
 *
 * <p>The annotations live on this base class so that every subclass produces the <em>same</em>
 * merged context configuration and Spring's context cache hands them one application context and
 * one broker between them. Adding a property to a single subclass would silently double the
 * suite's startup cost.</p>
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 3,
        topics = {Topics.GENERATED_ALERTS, Topics.AGGREGATED_TELEMETRY, Topics.DEAD_LETTER})
@TestPropertySource(properties = {
        // Point the application at the broker the test harness started.
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // Shorter poll timeout than production: the wire-contract test waits on a real round trip
        // through the broker, and a 3 second poll would dominate its runtime.
        "stream.alert.consumer.poll-timeout=250ms"
})
public abstract class AbstractIntegrationTest {

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected AlertRepository alertRepository;

    @Autowired
    protected StationWindowAggregateRepository aggregateRepository;

    /**
     * The context - and therefore the in-memory database - is shared across the integration test
     * classes, so each test starts from a known empty store rather than inheriting whatever the
     * previous class left behind.
     */
    @BeforeEach
    void clearStore() {
        alertRepository.deleteAll();
        aggregateRepository.deleteAll();
    }
}
