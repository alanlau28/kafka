/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.FaultRule;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * RUNTIME chaos harness — bug HUNT (not a regression guard). A single EOS Streams app stays up for the whole
 * test; faults are injected LIVE via the wire proxy to churn task lifecycle (no {@code close()}/restart). The
 * goal is to surface NEW defects of the escaped-defect class (KIP-1035 offset management × task/store
 * lifecycle) — e.g. a fatal from a recoverable condition, or silent exactly-once loss/duplication.
 *
 * <p>This reference lever: intermittent {@code PRODUCER_FENCED} on {@code END_TXN} ({@code withProbability})
 * → {@code TaskMigratedException} → {@code closeDirty} + re-init on the SAME live store objects, repeatedly,
 * while processing continues. Other levers (restore-OOR TaskCorrupted storm, cleaner race, combined soak)
 * follow this same scaffold in sibling classes.
 *
 * <p>Store under test: a persistent WINDOWED (segmented) store — the RocksDB-segment family the escaped
 * defects lived in. Oracle (checked after the storm, once the app quiesces): (1) no fatal surfaced to the
 * uncaught handler — in particular none with the KAFKA-20808 committedOffset-on-closed-segment signature;
 * (2) EXACTLY-ONCE — the sum of all window counts (read via IQ from the store itself) equals the number of
 * records produced, i.e. no record lost or double-counted across the churn; (3) liveness — RUNNING at the end.
 */
@Tag("integration")
@Timeout(360)
public class RuntimeChaosCombinedSoakIntegrationTest {

    private static final String STORE_NAME = "chaos-windowed-counts";
    private static final String[] KEYS = {"a", "b", "c"};
    private static final long WINDOW_MS = 200L;
    private static final long RETENTION_MS = 600_000L; // >> test duration, so no window expires before the IQ check
    private static final long CHAOS_DURATION_MS = 90_000L;
    private static final long PRODUCE_INTERVAL_MS = 25L;
    private static final double FAULT_PROBABILITY = 0.30;

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String inputTopic;
    private String appId;
    private KafkaStreams streams;
    private KafkaProducer<String, String> producer;

    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();
    private final AtomicLong produced = new AtomicLong(0);
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private Thread producerThread;

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "chaos-combined-" + base;
        inputTopic = appId + "-in";
        cluster.createTopic(inputTopic, 2, 1); // 2 partitions -> 2 tasks, more lifecycle churn surface
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());

        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(p);
    }

    @AfterEach
    public void tearDown() throws Exception {
        producing.set(false);
        if (producerThread != null) {
            producerThread.join(Duration.ofSeconds(10).toMillis());
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldStayExactlyOnceUnderCombinedRuntimeChaos() throws Exception {
        startApp();
        startProducer();

        // Combined runtime chaos: several faults armed at once, all withProbability, so rare interleavings can
        // occur (a corruption landing mid-migration, a commit gap during a revive, a produce retry across a
        // rebalance). Tuned so the app still makes progress and can drain when cleared. NO restart.
        final FaultRule restoreOor = proxy.injectError(ApiKeys.FETCH, Errors.OFFSET_OUT_OF_RANGE)
            .forClient("restore").withProbability(0.5);
        final FaultRule fence = proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED)
            .withProbability(0.2);
        final FaultRule epoch = proxy.injectError(ApiKeys.END_TXN, Errors.INVALID_PRODUCER_EPOCH)
            .withProbability(0.1);
        final FaultRule commitGap = proxy.disconnectOn(ApiKeys.END_TXN).withProbability(0.05);
        final FaultRule produceRetry = proxy.injectError(ApiKeys.PRODUCE, Errors.NOT_ENOUGH_REPLICAS)
            .withProbability(0.1);
        final LogCaptureAppender logs = LogCaptureAppender.createAndRegister();
        Thread.sleep(CHAOS_DURATION_MS);

        // Stop the storm and the producer, then let the app fully drain and quiesce.
        proxy.clearFaults();
        producing.set(false);
        producerThread.join(Duration.ofSeconds(10).toMillis());

        final long churnLogs = logs.getMessages().stream().filter(m -> {
            final String l = m.toLowerCase(Locale.ROOT);
            return l.contains("corrupt") || l.contains("wiped") || l.contains("reviv")
                || l.contains("migrated") || l.contains("fenced");
        }).count();
        logs.close();

        final long fired = restoreOor.timesTriggered() + fence.timesTriggered() + epoch.timesTriggered()
            + commitGap.timesTriggered() + produceRetry.timesTriggered();
        final long total = produced.get();
        // Oracle 2: exactly-once. Poll the store (via IQ) until the summed window counts reach the produced
        // total, or time out. Under exactly-once this converges to EXACTLY total; > total => duplication.
        final long summed = waitForStoreSum(total);

        System.out.println("CHAOS-STATS fired=" + fired + " (oor=" + restoreOor.timesTriggered() + " fence="
            + fence.timesTriggered() + " epoch=" + epoch.timesTriggered() + " commitGap="
            + commitGap.timesTriggered() + " produce=" + produceRetry.timesTriggered() + ") churnLogs=" + churnLogs
            + " produced=" + total + " summed=" + summed);

        // Churn sanity: the combined storm must have fired AND churned lifecycle, else the pass is hollow.
        assertTrue(fired > 0, "the combined storm never fired (fired=" + fired + ")");
        assertTrue(churnLogs > 0, "no corruption/migration churn observed in logs (count=" + churnLogs + ")");

        // Oracle 1: no fatal from a recoverable churn.
        final Throwable fatal = uncaught.get();
        if (fatal != null) {
            final String chain = throwableChain(fatal).toLowerCase(Locale.ROOT);
            if (chain.contains("committedoffset") || (chain.contains("closed") && chain.contains("segment"))) {
                fail("KAFKA-20808-class regression under combined chaos:\n" + throwableChain(fatal));
            }
            fail("combined runtime chaos surfaced a fatal exception:\n" + throwableChain(fatal));
        }
        assertEquals(total, summed,
            "exactly-once violated under combined runtime chaos: produced=" + total + " but store summed=" + summed);
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after the storm");
    }

    // --- scaffold ---

    private void startApp() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final WindowBytesStoreSupplier supplier = Stores.persistentWindowStore(
            STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_MS), false);
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(WINDOW_MS)))
            .count(Materialized.as(supplier));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.start();
        waitForRunning(Duration.ofSeconds(120).toMillis());
    }

    private void startProducer() {
        producing.set(true);
        producerThread = new Thread(() -> {
            int i = 0;
            while (producing.get()) {
                final String key = KEYS[i % KEYS.length];
                producer.send(new ProducerRecord<>(inputTopic, null, System.currentTimeMillis(), key, "v"));
                produced.incrementAndGet();
                i++;
                try {
                    Thread.sleep(PRODUCE_INTERVAL_MS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            producer.flush();
        }, "chaos-producer");
        producerThread.setDaemon(true);
        producerThread.start();
    }

    private void waitForRunning(final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                return;
            }
            if (uncaught.get() != null) {
                fail("app died before reaching RUNNING:\n" + throwableChain(uncaught.get()));
            }
            Thread.sleep(200);
        }
        fail("app did not reach RUNNING within " + timeoutMs + "ms (state=" + streams.state() + ")");
    }

    /** Sum all window counts across all keys via IQ. Retries through transient IQ unavailability during rebalances. */
    private long storeSum() {
        final ReadOnlyWindowStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));
        long sum = 0;
        try (KeyValueIterator<org.apache.kafka.streams.kstream.Windowed<String>, Long> all = store.all()) {
            while (all.hasNext()) {
                sum += all.next().value;
            }
        }
        return sum;
    }

    private long waitForStoreSum(final long target) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = storeSum();
                if (last >= target) {
                    return last;
                }
            } catch (final InvalidStateStoreException retry) {
                // store temporarily unavailable (rebalance/revive in progress) — keep polling
            }
            Thread.sleep(500);
        }
        return last;
    }

    private static String throwableChain(final Throwable t) {
        final StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append('\n');
            cur = cur.getCause();
        }
        return sb.toString();
    }

}
