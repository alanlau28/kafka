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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * RUNTIME chaos bug HUNT — TWO EOS instances + standby, driven into a near-constant rebalance storm punctuated
 * by brief calm windows, to surface NEW defects in the cross-instance task-handoff / standby-promotion paths
 * that a single-instance harness cannot reach (that is where KIP-1035 changelog-offset ownership changes hands).
 *
 * <p>Topology: 6-partition source -> session-windowed count (segmented RocksDB store, the KAFKA-20808 family)
 * -> sink. Two {@link KafkaStreams} instances, 1 stream thread each, {@code num.standby.replicas=1}, EOS-v2, so
 * each instance owns 3 active + 3 standby tasks and every active-task move between instances transits a standby.
 *
 * <p>Chaos: consumer/txn timeouts tuned DOWN and a low {@code probing.rebalance.interval.ms} so warmup/standby
 * churn happens even without faults; on top, the wire proxy injects transaction/heartbeat faults in WAVES —
 * churn window (faults armed) then calm window (faults cleared). NO {@code close()}/restart of either instance.
 *
 * <p>Bug bar (strongest): FAIL if (1) either instance surfaces a fatal / dies, or (2) sink output STALLS during
 * a calm window (running-but-can't-process — the silent-death class), or (3) the group never reconverges after
 * the storm, or (4) the final exactly-once reconciliation from the sink != records produced. A running instance
 * with zero assigned tasks counts as (2).
 *
 * <p>Non-hollow bar: assert the storm actually churned — a standby->active promotion occurred (active-task
 * ownership moved between the two instances, sampled by a monitor thread; with num.standby=1 that is a
 * promotion) — else a clean pass is meaningless.
 */
@Tag("integration")
@Timeout(600)
public class RuntimeChaosStandbyRebalanceStormIntegrationTest {

    private static final String STORE_NAME = "chaos-session-counts";
    private static final int PARTITIONS = 6;
    private static final String[] KEYS = {"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11"};
    private static final long SESSION_GAP_MS = 5_000L;
    private static final long PRODUCE_INTERVAL_MS = 25L;
    private static final int WAVES = 4;
    private static final long CHURN_MS = 20_000L;
    private static final long CALM_MS = 25_000L;
    private static final long STALL_THRESHOLD_MS = 40_000L; // max tolerated no-progress DURING a calm window

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private KafkaProducer<String, String> producer;

    private KafkaStreams instanceA;
    private KafkaStreams instanceB;

    private final AtomicReference<Throwable> uncaughtA = new AtomicReference<>();
    private final AtomicReference<Throwable> uncaughtB = new AtomicReference<>();
    private final AtomicLong produced = new AtomicLong(0);
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private final AtomicBoolean sinkConsuming = new AtomicBoolean(false);
    private final AtomicBoolean monitoring = new AtomicBoolean(false);
    private Thread producerThread;
    private Thread sinkThread;
    private Thread monitorThread;

    // Sink state, maintained by the background sink consumer: latest value per encoded session-window key
    // (null value = session removed/tombstoned). Under exactly-once, sum of the surviving values == produced.
    private final Map<String, Long> latestPerWindow = new ConcurrentHashMap<>();
    private final AtomicLong sinkRecords = new AtomicLong(0);

    // Promotion proof: last-seen active-owner instance ("A"/"B") per TaskId, and a count of ownership moves.
    private final Map<TaskId, String> activeOwner = new HashMap<>();
    private final AtomicLong ownershipMoves = new AtomicLong(0);

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "chaos-standby-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        cluster.createTopic(inputTopic, PARTITIONS, 1);
        cluster.createTopic(outputTopic, PARTITIONS, 1);
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
        sinkConsuming.set(false);
        monitoring.set(false);
        joinQuietly(producerThread);
        joinQuietly(monitorThread);
        joinQuietly(sinkThread);
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        if (instanceA != null) {
            instanceA.close(Duration.ofSeconds(30));
        }
        if (instanceB != null) {
            instanceB.close(Duration.ofSeconds(30));
        }
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldStayLiveAndExactlyOnceUnderStandbyRebalanceStorm() throws Exception {
        instanceA = buildInstance();
        instanceB = buildInstance();
        instanceA.setUncaughtExceptionHandler(handler(uncaughtA));
        instanceB.setUncaughtExceptionHandler(handler(uncaughtB));
        instanceA.start();
        instanceB.start();

        startProducer();
        startSinkConsumer();
        startMonitor();

        // Wait for the group to reach an initial stable, processing state before the storm.
        awaitBothRunningAndProgressing(Duration.ofSeconds(120).toMillis());

        try (LogCaptureAppender logs = LogCaptureAppender.createAndRegister()) {
            for (int wave = 1; wave <= WAVES; wave++) {
                armRandomFaults();
                Thread.sleep(CHURN_MS);

                // Calm window: clear faults, then output MUST resume/advance within the stall threshold.
                proxy.clearFaults();
                assertProgressDuringCalm(wave);
                assertNoFatal(wave);
            }

            final long recycleLogs = logs.getMessages().stream()
                .filter(m -> m.toLowerCase(Locale.ROOT).contains("recycled state")).count();
            System.out.println("CHAOS-STATS ownershipMoves=" + ownershipMoves.get() + " recycleLogs=" + recycleLogs);

            // Non-hollow bar: a standby->active promotion (active-task ownership moved between instances) must
            // have occurred, else the storm didn't exercise the handoff path.
            assertTrue(ownershipMoves.get() > 0,
                "no standby->active promotion observed (active-task ownership never moved between instances)");
        }

        // Final phase: stop faults + producing, require the group to RECONVERGE and drain, then reconcile.
        proxy.clearFaults();
        producing.set(false);
        joinQuietly(producerThread);
        producer.flush();

        awaitBothRunningAndProgressing(Duration.ofSeconds(120).toMillis());
        final long total = produced.get();
        final long summed = awaitSinkSum(total, Duration.ofSeconds(120).toMillis());

        System.out.println("CHAOS-STATS-FINAL produced=" + total + " summed=" + summed
            + " ownershipMoves=" + ownershipMoves.get() + " sinkRecords=" + sinkRecords.get());

        assertNoFatal(-1);
        assertEquals(KafkaStreams.State.RUNNING, instanceA.state(), "instance A should be RUNNING at the end");
        assertEquals(KafkaStreams.State.RUNNING, instanceB.state(), "instance B should be RUNNING at the end");
        assertEquals(total, summed,
            "exactly-once violated across the rebalance storm: produced=" + total + " but sink summed=" + summed);
    }

    // --- chaos ---

    private void armRandomFaults() {
        proxy.clearFaults();
        // A menu of recoverable, rebalance-driving faults; arm a random subset each wave (all withProbability).
        if (ThreadLocalRandom.current().nextBoolean()) {
            proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED).withProbability(0.25);
        }
        if (ThreadLocalRandom.current().nextBoolean()) {
            proxy.injectError(ApiKeys.END_TXN, Errors.INVALID_PRODUCER_EPOCH).withProbability(0.15);
        }
        if (ThreadLocalRandom.current().nextBoolean()) {
            proxy.disconnectOn(ApiKeys.HEARTBEAT).withProbability(0.20);
        }
        if (ThreadLocalRandom.current().nextBoolean()) {
            proxy.disconnectOn(ApiKeys.TXN_OFFSET_COMMIT).withProbability(0.10);
        }
        // Always keep at least one active driver so a wave never no-ops.
        proxy.injectError(ApiKeys.FETCH, Errors.OFFSET_OUT_OF_RANGE).forClient("restore").withProbability(0.30);
    }

    // --- oracles ---

    private void assertProgressDuringCalm(final int wave) throws Exception {
        final long start = sinkRecords.get();
        final long deadline = System.currentTimeMillis() + CALM_MS;
        long lastSeen = start;
        long lastProgressAt = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            final long now = sinkRecords.get();
            if (now > lastSeen) {
                lastSeen = now;
                lastProgressAt = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - lastProgressAt > STALL_THRESHOLD_MS) {
                fail("wave " + wave + ": sink output STALLED for >" + STALL_THRESHOLD_MS
                    + "ms during a calm window (running-but-cannot-process). sinkRecords stuck at " + lastSeen
                    + "; A=" + describe(instanceA, uncaughtA) + " B=" + describe(instanceB, uncaughtB));
            }
            Thread.sleep(500);
        }
        assertTrue(sinkRecords.get() > start,
            "wave " + wave + ": no sink output produced during the entire calm window (start=" + start
                + " end=" + sinkRecords.get() + ")");
    }

    private void assertNoFatal(final int wave) {
        final String ctx = wave < 0 ? "final" : ("wave " + wave);
        checkFatal(ctx, "A", uncaughtA.get());
        checkFatal(ctx, "B", uncaughtB.get());
    }

    private void checkFatal(final String ctx, final String which, final Throwable t) {
        if (t == null) {
            return;
        }
        final String chain = throwableChain(t).toLowerCase(Locale.ROOT);
        if (chain.contains("committedoffset") || (chain.contains("closed") && chain.contains("segment"))) {
            fail(ctx + ": KAFKA-20808-class fatal on instance " + which + ":\n" + throwableChain(t));
        }
        fail(ctx + ": instance " + which + " surfaced a fatal exception:\n" + throwableChain(t));
    }

    // --- scaffold ---

    private KafkaStreams buildInstance() {
        final StreamsBuilder builder = new StreamsBuilder();
        final SessionBytesStoreSupplier supplier =
            Stores.persistentSessionStore(STORE_NAME, Duration.ofMillis(SESSION_GAP_MS * 6));
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(SESSION_GAP_MS)))
            .count(Materialized.as(supplier))
            .toStream()
            // Encode the full session-window identity into a plain string key; tombstones (null) flow through.
            .map((wk, v) -> new KeyValue<>(wk.key() + "|" + wk.window().start() + "|" + wk.window().end(), v))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000L);
        // Drive standby<->active churn on its own (60s is the enforced minimum).
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 60_000L);
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 100L);
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 6);
        // Timeouts tuned DOWN for aggressive eviction/rebalance, but transaction.timeout > commit interval.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6_000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 2_000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 30_000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 15_000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 10_000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaStreams(builder.build(), props);
    }

    private StreamsUncaughtExceptionHandler handler(final AtomicReference<Throwable> sink) {
        return t -> {
            sink.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        };
    }

    private void startProducer() {
        producing.set(true);
        producerThread = new Thread(() -> {
            int i = 0;
            while (producing.get()) {
                final String key = KEYS[i % KEYS.length];
                producer.send(new ProducerRecord<>(inputTopic, null, System.currentTimeMillis(), key, "v"));
                producer.flush();
                produced.incrementAndGet();
                i++;
                sleepQuietly(PRODUCE_INTERVAL_MS);
            }
        }, "chaos-producer");
        producerThread.setDaemon(true);
        producerThread.start();
    }

    private void startSinkConsumer() {
        sinkConsuming.set(true);
        sinkThread = new Thread(() -> {
            final Properties c = new Properties();
            c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
            c.put(ConsumerConfig.GROUP_ID_CONFIG, "sink-" + appId);
            c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
            c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(c)) {
                consumer.subscribe(Collections.singletonList(outputTopic));
                while (sinkConsuming.get()) {
                    final ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                    for (final ConsumerRecord<String, Long> r : records) {
                        if (r.value() == null) {
                            latestPerWindow.remove(r.key());
                        } else {
                            latestPerWindow.put(r.key(), r.value());
                        }
                        sinkRecords.incrementAndGet();
                    }
                }
            }
        }, "chaos-sink-consumer");
        sinkThread.setDaemon(true);
        sinkThread.start();
    }

    private void startMonitor() {
        monitoring.set(true);
        monitorThread = new Thread(() -> {
            while (monitoring.get()) {
                sampleOwnership("A", instanceA);
                sampleOwnership("B", instanceB);
                sleepQuietly(1_000L);
            }
        }, "chaos-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private synchronized void sampleOwnership(final String which, final KafkaStreams ks) {
        if (ks == null || ks.state() != KafkaStreams.State.RUNNING) {
            return;
        }
        try {
            for (final TaskId id : activeTaskIds(ks)) {
                final String prev = activeOwner.put(id, which);
                if (prev != null && !prev.equals(which)) {
                    ownershipMoves.incrementAndGet();
                }
            }
        } catch (final Exception ignore) {
            // metadata momentarily unavailable during a rebalance — skip this sample
        }
    }

    private Set<TaskId> activeTaskIds(final KafkaStreams ks) {
        final Set<TaskId> ids = new HashSet<>();
        for (final ThreadMetadata tm : ks.metadataForLocalThreads()) {
            tm.activeTasks().forEach(t -> ids.add(t.taskId()));
        }
        return ids;
    }

    private void awaitBothRunningAndProgressing(final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            checkFatal("await", "A", uncaughtA.get());
            checkFatal("await", "B", uncaughtB.get());
            final boolean bothRunning = instanceA.state() == KafkaStreams.State.RUNNING
                && instanceB.state() == KafkaStreams.State.RUNNING;
            if (bothRunning) {
                // require the whole partition set to be actively owned (no orphaned partitions)
                final Set<TaskId> owned = new HashSet<>(activeTaskIds(instanceA));
                owned.addAll(activeTaskIds(instanceB));
                if (owned.size() >= PARTITIONS) {
                    final long before = sinkRecords.get();
                    Thread.sleep(2_000);
                    if (sinkRecords.get() > before) {
                        return;
                    }
                }
            }
            Thread.sleep(500);
        }
        fail("group did not reconverge to RUNNING + full active ownership + progress within " + timeoutMs
            + "ms; A=" + describe(instanceA, uncaughtA) + " B=" + describe(instanceB, uncaughtB));
    }

    private long awaitSinkSum(final long target, final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        long sum = -1;
        while (System.currentTimeMillis() < deadline) {
            sum = currentSinkSum();
            if (sum >= target) {
                return sum;
            }
            Thread.sleep(1_000);
        }
        return sum;
    }

    private long currentSinkSum() {
        long sum = 0;
        for (final Long v : new ArrayList<>(latestPerWindow.values())) {
            if (v != null) {
                sum += v;
            }
        }
        return sum;
    }

    private String describe(final KafkaStreams ks, final AtomicReference<Throwable> u) {
        String tasks = "?";
        try {
            tasks = String.valueOf(activeTaskIds(ks).size());
        } catch (final Exception ignore) {
            // ignore
        }
        return "[state=" + ks.state() + " activeTasks=" + tasks + " fatal=" + (u.get() != null) + "]";
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

    private static void sleepQuietly(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinQuietly(final Thread t) {
        if (t == null) {
            return;
        }
        try {
            t.join(Duration.ofSeconds(15).toMillis());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
