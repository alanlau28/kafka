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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * RUNTIME chaos harness for the state-directory cleaner race (KAFKA-20805 family).
 *
 * <p>KIP-1035 moved changelog offsets from the per-task {@code .checkpoint} child file into an "offsets"
 * RocksDB column family inside the task's RocksDB directory, so the task dir mtime no longer advances on
 * every commit the way the checkpoint rewrite used to. The background
 * {@link org.apache.kafka.streams.processor.internals.StateDirectory} cleaner deletes a task dir when
 * {@code now - cleanupDelayMs > dir.lastModified()} provided the dir is not locked by a local thread.
 *
 * <p>To genuinely ORPHAN a task dir (so the eager cleaner deletes it, then the task migrates back and must
 * re-init / re-restore -- the KIP-1035 offsets-column-family risk) this test runs TWO single-thread
 * {@link KafkaStreams} instances in the same group. Shedding a thread from instance B moves its task to
 * instance A with no local re-owner on B, so B's dir goes unlocked and the eager cleaner deletes it; adding
 * the thread back forces the task to re-init on a wiped dir.
 *
 * <p>Oracles after the storm: (1) no fatal to the uncaught handler -- especially none with the KAFKA-20808
 * committedOffset-on-closed-segment signature; (2) exactly-once -- summed per-key counts (via IQ across both
 * instances) == records produced; (3) liveness. Non-hollow bar: the cleaner must actually have deleted a dir
 * AND migrations must have churned.
 */
@Tag("integration")
@Timeout(420)
public class RuntimeChaosCleanerRaceIntegrationTest {

    private static final String STORE_NAME = "chaos-kv-counts";
    private static final String[] KEYS = {"a", "b", "c", "d", "e", "f"};
    private static final long PRODUCE_INTERVAL_MS = 25L;
    private static final double FENCE_PROBABILITY = 0.30;
    private static final double DISCONNECT_PROBABILITY = 0.15;
    private static final long CLEANUP_DELAY_MS = 1_000L;
    private static final int CHURN_CYCLES = 8;
    private static final long SHED_WINDOW_MS = 8_000L; // orphan window: comfortably exceeds a cleaner scan interval
    private static final long SETTLE_MS = 3_000L;      // dwell after re-adding the thread before the next shed

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String inputTopic;
    private String appId;
    private KafkaStreams streamsA;
    private KafkaStreams streamsB;
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
        appId = "chaos-cleaner-" + base;
        inputTopic = appId + "-in";
        cluster.createTopic(inputTopic, 6, 1); // 6 tasks to shuffle across two single-thread instances
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
        if (streamsA != null) {
            streamsA.close(Duration.ofSeconds(30));
        }
        if (streamsB != null) {
            streamsB.close(Duration.ofSeconds(30));
        }
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldStayExactlyOnceWhileCleanerDeletesOrphanedTaskDirs() throws Exception {
        final LogCaptureAppender logs = LogCaptureAppender.createAndRegister();
        streamsA = buildStreams(TestUtils.tempDirectory().getPath());
        streamsB = buildStreams(TestUtils.tempDirectory().getPath());
        streamsA.start();
        streamsB.start();
        waitForBothRunning(Duration.ofSeconds(60).toMillis());
        startProducer();

        final FaultRule fence =
            proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED).withProbability(FENCE_PROBABILITY);
        final FaultRule hb =
            proxy.disconnectOn(ApiKeys.HEARTBEAT).withProbability(DISCONNECT_PROBABILITY);
        runCleanerChurn();

        proxy.clearFaults();
        producing.set(false);
        producerThread.join(Duration.ofSeconds(10).toMillis());

        final long cleanerDeletions = countLogs(logs, "deleting obsolete state directory");
        final long migrationLogs = countMigrationLogs(logs);
        final long restoreLogs = countRestoreLogs(logs);
        logs.close();

        final long total = produced.get();
        final long summed = waitForStoreSum(total);

        System.out.println("CHAOS-STATS fired=" + fence.timesTriggered()
            + " hbDisconnects=" + hb.timesTriggered()
            + " cleanerDeletions=" + cleanerDeletions
            + " migrationLogs=" + migrationLogs
            + " restoreLogs=" + restoreLogs
            + " produced=" + total + " summed=" + summed);

        assertTrue(fence.timesTriggered() > 0,
            "the fence storm never fired (fired=" + fence.timesTriggered() + ")");
        assertTrue(migrationLogs > 0,
            "no migration/fence churn observed in logs (count=" + migrationLogs + ")");
        assertTrue(cleanerDeletions > 0,
            "the state-directory cleaner never fired (cleanerDeletions=" + cleanerDeletions
                + ") -- lever not exercised");

        final Throwable fatal = uncaught.get();
        if (fatal != null) {
            final String chain = throwableChain(fatal).toLowerCase(Locale.ROOT);
            if (chain.contains("committedoffset") || (chain.contains("closed") && chain.contains("segment"))) {
                fail("KAFKA-20808-class regression under cleaner-race storm:\n" + throwableChain(fatal));
            }
            fail("runtime cleaner-race storm surfaced a fatal exception:\n" + throwableChain(fatal));
        }
        assertEquals(total, summed,
            "exactly-once violated under cleaner-race storm: produced=" + total + " but store summed=" + summed);
        assertTrue(streamsA.state() == KafkaStreams.State.RUNNING || streamsB.state() == KafkaStreams.State.RUNNING,
            "at least one instance should be RUNNING after the storm");
    }

    // --- scaffold ---

    private void runCleanerChurn() throws Exception {
        for (int i = 0; i < CHURN_CYCLES && uncaught.get() == null; i++) {
            try {
                final Optional<String> removed = streamsB.removeStreamThread();
                removed.ifPresent(n -> { });
            } catch (final Exception ignore) {
                // shedding may race a rebalance; ignore and continue churning
            }
            Thread.sleep(SHED_WINDOW_MS); // orphan window: task moves to A, B's dir unlocked -> cleaner deletes it
            try {
                streamsB.addStreamThread();
            } catch (final Exception ignore) {
                // adding may race a rebalance; ignore
            }
            Thread.sleep(SETTLE_MS);
        }
    }

    private KafkaStreams buildStreams(final String stateDirPath) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore(STORE_NAME))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, true); // KIP-892 transactional state stores (the path under test)
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, CLEANUP_DELAY_MS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        return ks;
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

    private void waitForBothRunning(final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (streamsA.state() == KafkaStreams.State.RUNNING
                && streamsB.state() == KafkaStreams.State.RUNNING) {
                return;
            }
            if (uncaught.get() != null) {
                fail("app died before reaching RUNNING:\n" + throwableChain(uncaught.get()));
            }
            Thread.sleep(200);
        }
        fail("apps did not both reach RUNNING within " + timeoutMs + "ms (A="
            + streamsA.state() + ", B=" + streamsB.state() + ")");
    }

    private static long countLogs(final LogCaptureAppender logs, final String substr) {
        return logs.getMessages().stream()
            .filter(m -> m.toLowerCase(Locale.ROOT).contains(substr))
            .count();
    }

    private static long countMigrationLogs(final LogCaptureAppender logs) {
        return logs.getMessages().stream().filter(m -> {
            final String l = m.toLowerCase(Locale.ROOT);
            return l.contains("migrated") || l.contains("fenced") || l.contains("detected that the thread");
        }).count();
    }

    private static long countRestoreLogs(final LogCaptureAppender logs) {
        return logs.getMessages().stream().filter(m -> {
            final String l = m.toLowerCase(Locale.ROOT);
            return l.contains("restoring state") || l.contains("restoration in progress")
                || l.contains("state directory does not exist") || l.contains("reinitializing");
        }).count();
    }

    /** Sum all per-key counts via IQ across BOTH instances (a key lives on exactly one instance). */
    private long storeSum() {
        long sum = 0;
        for (final KafkaStreams ks : new KafkaStreams[] {streamsA, streamsB}) {
            try {
                final ReadOnlyKeyValueStore<String, Long> store = ks.store(
                    StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.<String, Long>keyValueStore()));
                try (KeyValueIterator<String, Long> all = store.all()) {
                    while (all.hasNext()) {
                        sum += all.next().value;
                    }
                }
            } catch (final InvalidStateStoreException skip) {
                // this instance does not (yet) serve the store -- rebalance/revive in progress
            }
        }
        return sum;
    }

    private long waitForStoreSum(final long target) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = storeSum();
            if (last >= target) {
                return last;
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
