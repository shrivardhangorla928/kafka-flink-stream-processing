package com.vassar.aware.flink.testing;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Captures what a stream emitted, so a job-level test can assert on the real topology without a
 * Kafka broker.
 *
 * <p>Results land in a static registry keyed by name because the sink instance is serialised to
 * the task and the copy the test holds is not the copy that runs. That is safe here only because
 * the mini cluster runs in this JVM; it is a test affordance and nothing else.</p>
 *
 * @param <T> element type being collected
 */
public final class CollectingSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private static final Map<String, Queue<Object>> COLLECTED = new ConcurrentHashMap<>();

    private final String name;

    public CollectingSink(String name) {
        this.name = name;
        COLLECTED.computeIfAbsent(name, key -> new ConcurrentLinkedQueue<>());
    }

    @Override
    @SuppressWarnings("deprecation") // the non-deprecated overload is a default method in 1.20
    public SinkWriter<T> createWriter(InitContext context) {
        return new CollectingWriter<>(name);
    }

    /** Everything collected under a name, in arrival order. */
    @SuppressWarnings("unchecked")
    public static <T> List<T> collected(String name) {
        Queue<Object> queue = COLLECTED.get(name);
        return queue == null ? List.of() : new ArrayList<>((Queue<T>) queue);
    }

    /** Drops previous results so tests do not observe each other's output. */
    public static void reset(String name) {
        COLLECTED.remove(name);
    }

    /** Top-level rather than anonymous, so it never captures the enclosing test instance. */
    private static final class CollectingWriter<T> implements SinkWriter<T> {

        private final String name;

        private CollectingWriter(String name) {
            this.name = name;
        }

        @Override
        public void write(T element, Context context) {
            COLLECTED.computeIfAbsent(name, key -> new ConcurrentLinkedQueue<>()).add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // nothing buffered
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
