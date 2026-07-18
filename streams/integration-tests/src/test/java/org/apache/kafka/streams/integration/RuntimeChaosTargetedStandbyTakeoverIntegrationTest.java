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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * RUNTIME chaos bug HUNT — TARGETED standby takeover. Proves that when the active owner of a specific set of
 * partitions is killed, the WARM STANDBY on the surviving instance actually takes over those exact partitions
 * and keeps serving them — not merely that the group survives.
 *
 * <p>Two EOS-v2 instances (distinct {@code client.id} "instA"/"instB"), 1 thread each,
 * {@code num.standby.replicas=1}, over a 6-partition session-windowed count (segmented store). Each instance
 * owns 3 active + 3 standby, and (num.standby=1) the surviving instance's standby set for the victim's tasks
 * is exactly warm.
 *
 * <p>The "kill" is a pure wire fault: {@link KafkaProtocolFaultProxy#blackholeClient} drops the victim
 * instance's requests before they reach the broker (a one-node network partition), so the broker evicts it by
 * session timeout — the ungraceful-crash path where KIP-1035 offset handoff is actually stressed. NO
 * {@code close()}/restart. Because the blackhole is reversible, we do it BOTH ways (kill A -> B serves A's
 * partitions; recover; kill B -> A serves B's partitions) for symmetry.
 *
 * <p>Per takeover the oracle asserts: (1) the surviving instance's ACTIVE task set comes to include the exact
 * TaskIds the victim owned (its warm standby was promoted to active for those partitions); (2) sink output
 * keeps advancing while the victim is partitioned (the survivor is actually processing them, not stalled);
 * (3) the survivor stays RUNNING with no fatal (no KAFKA-20808 committedOffset-on-closed-segment signature).
 * Final: after both recover and drain, exactly-once holds (sink sum == produced), proving the takeovers lost
 * or duplicated nothing.
 */
@Tag("integration")
@Timeout(600)
public class RuntimeChaosTargetedStandbyTakeoverIntegrationTest {

    private static final String STORE_NAME = "takeover-session-counts";
    private static final int PARTITIONS = 6;
    private static final String[] KEYS = {"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11"};
    private static final long SESSION_GAP_MS = 5_000L;
    private static final long PRODUCE_INTERVAL_MS = 25L;
    private static final long TAKEOVER_TIMEOUT_MS = 90_000L; // eviction (session timeout) + promotion
    private static final long REWARM_TIMEOUT_MS = 120_000L;  // victim rejoins + rebuilds a warm standby

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
    private Thread producerThread;
    private Thread sinkThread;

    private final Map<String, Long> latestPerWindow = new ConcurrentHashMap<>();
    private final AtomicLong sinkRecords = new AtomicLong(0);

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "chaos-takeover-" + base;
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
        joinQuietly(producerThread);
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
    public void shouldPromoteWarmStandbyForTheExactPartitionsWhenActiveOwnerIsKilled() throws Exception {
        instanceA = buildInstance("instA");
        instanceB = buildInstance("instB");
        instanceA.setUncaughtExceptionHandler(handler(uncaughtA));
        instanceB.setUncaughtExceptionHandler(handler(uncaughtB));
        instanceA.start();
        instanceB.start();

        startProducer();
        startSinkConsumer();

        // Warm up: both RUNNING, all 6 partitions actively owned, and each instance holds the other's actives
        // as warm standbys.
        awaitWarmStandbys(Duration.ofSeconds(150).toMillis());
        // Reaching warm steady state means any transient startup condition (e.g. stale-metadata
        // MissingSourceTopicException, auto-retried) has healed under REPLACE_THREAD — clear it so the takeover
        // phases start from a clean slate and only takeover-time defects count.
        uncaughtA.set(null);
        uncaughtB.set(null);

        // Cycle 1: kill A, assert B's warm standby serves A's exact partitions.
        killAndAssertTakeover("instA", instanceA, uncaughtA, instanceB, uncaughtB, "B");
        // Recover A, let the group re-warm both ways.
        proxy.clearFaults();
        awaitWarmStandbys(REWARM_TIMEOUT_MS);

        // Cycle 2 (symmetry): kill B, assert A's warm standby serves B's exact partitions.
        killAndAssertTakeover("instB", instanceB, uncaughtB, instanceA, uncaughtA, "A");
        proxy.clearFaults();
        awaitWarmStandbys(REWARM_TIMEOUT_MS);

        // Final reconciliation: stop producing, drain, assert exactly-once (no loss/dup across the takeovers).
        producing.set(false);
        joinQuietly(producerThread);
        producer.flush();
        final long total = produced.get();
        final long summed = awaitSinkSum(total, Duration.ofSeconds(120).toMillis());
        System.out.println("CHAOS-STATS-FINAL produced=" + total + " summed=" + summed
            + " sinkRecords=" + sinkRecords.get());

        checkFatal("final", "A", uncaughtA.get());
        checkFatal("final", "B", uncaughtB.get());
        assertEquals(total, summed,
            "exactly-once violated across the targeted takeovers: produced=" + total + " sink summed=" + summed);
    }

    /**
     * Blackhole {@code victimClientId}, then assert the survivor promotes its warm standby to active for the
     * victim's exact partitions and keeps processing them.
     */
    private void killAndAssertTakeover(final String victimClientId,
                                       final KafkaStreams victim,
                                       final AtomicReference<Throwable> victimFatal,
                                       final KafkaStreams survivor,
                                       final AtomicReference<Throwable> survivorFatal,
                                       final String survivorName) throws Exception {
        final Set<TaskId> target = activeTaskIds(victim);
        assertTrue(target.size() >= 1, "victim " + victimClientId + " should own at least one active task");
        assertTrue(standbyTaskIds(survivor).containsAll(target),
            "survivor must hold a WARM STANDBY for the victim's partitions before the kill (target=" + target
                + " survivorStandbys=" + standbyTaskIds(survivor) + ")");

        final long baseline = sinkRecords.get();
        proxy.blackholeClient(victimClientId); // ungraceful one-node partition -> broker evicts by session timeout

        // The survivor's ACTIVE set must come to include the exact target partitions (standby -> active).
        final long deadline = System.currentTimeMillis() + TAKEOVER_TIMEOUT_MS;
        boolean promoted = false;
        while (System.currentTimeMillis() < deadline) {
            checkFatal("takeover(" + victimClientId + ")", survivorName, survivorFatal.get());
            if (survivor.state() == KafkaStreams.State.RUNNING && activeTaskIds(survivor).containsAll(target)) {
                promoted = true;
                break;
            }
            Thread.sleep(500);
        }
        if (!promoted) {
            fail("standby takeover FAILED: survivor " + survivorName + " did not promote the victim's exact "
                + "partitions to active within " + TAKEOVER_TIMEOUT_MS + "ms (target=" + target
                + " survivorActive=" + safeActive(survivor) + " survivorState=" + survivor.state() + ")");
        }

        // The survivor must actually PROCESS the taken-over partitions: sink output advances while the victim
        // is partitioned (only the survivor can be producing output now).
        final long progressDeadline = System.currentTimeMillis() + 45_000L;
        while (sinkRecords.get() <= baseline && System.currentTimeMillis() < progressDeadline) {
            checkFatal("takeover(" + victimClientId + ")", survivorName, survivorFatal.get());
            Thread.sleep(500);
        }
        assertTrue(sinkRecords.get() > baseline,
            "after the standby promoted the victim's partitions, sink output did NOT advance — survivor "
                + survivorName + " owns the tasks but is not processing them (baseline=" + baseline
                + " now=" + sinkRecords.get() + ")");

        System.out.println("CHAOS-STATS takeover victim=" + victimClientId + " target=" + target
            + " survivor=" + survivorName + " promoted=true sinkAdvanced=" + (sinkRecords.get() - baseline));
    }

    // --- scaffold ---

    private KafkaStreams buildInstance(final String clientId) {
        final StreamsBuilder builder = new StreamsBuilder();
        final SessionBytesStoreSupplier supplier =
            Stores.persistentSessionStore(STORE_NAME, Duration.ofMillis(SESSION_GAP_MS * 6));
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(SESSION_GAP_MS)))
            .count(Materialized.as(supplier))
            .toStream()
            .map((wk, v) -> new KeyValue<>(wk.key() + "|" + wk.window().start() + "|" + wk.window().end(), v))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId); // distinct per instance -> blackhole can target one
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000L);
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 100L);
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 6);
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
            // Keep the JVM thread recycling rather than killing the client, so a blackholed instance can rejoin
            // once the partition is lifted (the survivor is what we assert on).
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        };
    }

    private void awaitWarmStandbys(final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (instanceA.state() == KafkaStreams.State.RUNNING && instanceB.state() == KafkaStreams.State.RUNNING) {
                final Set<TaskId> aActive = safeActive(instanceA);
                final Set<TaskId> bActive = safeActive(instanceB);
                final Set<TaskId> aStandby = safeStandby(instanceA);
                final Set<TaskId> bStandby = safeStandby(instanceB);
                final Set<TaskId> allActive = new HashSet<>(aActive);
                allActive.addAll(bActive);
                // Full ownership (6 partitions) and FULL standby coverage: every active task has a warm standby
                // on the other instance. We do NOT require an even split — after a takeover the assignor may
                // leave all actives on the survivor with the recovered node as a full standby, which is a valid
                // precondition for the next (opposite-direction) kill.
                if (allActive.size() >= PARTITIONS
                    && bStandby.containsAll(aActive) && aStandby.containsAll(bActive)) {
                    return;
                }
            }
            Thread.sleep(500);
        }
        fail("group did not reach warm-standby steady state within " + timeoutMs + "ms; A=" + describe(instanceA)
            + " B=" + describe(instanceB));
    }

    /**
     * Poll the authoritative exactly-once oracle — the session STORE via IQ — until it reaches {@code target}
     * or times out. Every input record lands in exactly one session, so the sum of all session counts equals
     * records produced iff nothing was lost/duplicated. Immune to the session-output-stream orphan artifact
     * (a session's end shifts across a takeover, orphaning pre-takeover intermediate output records); a genuine
     * store over-count still surfaces via the final assertEquals.
     */
    private long awaitSinkSum(final long target, final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        long sum = -1;
        while (System.currentTimeMillis() < deadline) {
            final long s = storeSessionSum();
            if (s >= 0) {
                sum = s;
                if (sum >= target) {
                    return sum;
                }
            }
            Thread.sleep(1_000);
        }
        return sum;
    }

    private long storeSessionSum() {
        long sum = 0;
        for (final String key : KEYS) {
            final Long a = fetchSessionSum(instanceA, key);
            final Long b = fetchSessionSum(instanceB, key);
            if (a == null && b == null) {
                return -1; // neither instance can serve this key right now — retry the whole pass
            }
            sum += (a != null ? a : 0L) + (b != null ? b : 0L);
        }
        return sum;
    }

    private Long fetchSessionSum(final KafkaStreams ks, final String key) {
        try {
            final ReadOnlySessionStore<String, Long> store = ks.store(
                StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.sessionStore()));
            long s = 0;
            try (KeyValueIterator<Windowed<String>, Long> it = store.fetch(key)) {
                while (it.hasNext()) {
                    s += it.next().value;
                }
            }
            return s;
        } catch (final InvalidStateStoreException notHereRightNow) {
            return null;
        }
    }

    private Set<TaskId> activeTaskIds(final KafkaStreams ks) {
        final Set<TaskId> ids = new HashSet<>();
        for (final ThreadMetadata tm : ks.metadataForLocalThreads()) {
            tm.activeTasks().forEach(t -> ids.add(t.taskId()));
        }
        return ids;
    }

    private Set<TaskId> standbyTaskIds(final KafkaStreams ks) {
        final Set<TaskId> ids = new HashSet<>();
        for (final ThreadMetadata tm : ks.metadataForLocalThreads()) {
            tm.standbyTasks().forEach(t -> ids.add(t.taskId()));
        }
        return ids;
    }

    private Set<TaskId> safeActive(final KafkaStreams ks) {
        try {
            return activeTaskIds(ks);
        } catch (final Exception e) {
            return Collections.emptySet();
        }
    }

    private Set<TaskId> safeStandby(final KafkaStreams ks) {
        try {
            return standbyTaskIds(ks);
        } catch (final Exception e) {
            return Collections.emptySet();
        }
    }

    /**
     * With {@code REPLACE_THREAD} the app is designed to recover from thread exceptions, so a merely-recorded
     * exception is NOT a bug — recovery is asserted via liveness (RUNNING + takeover + sink progress) and
     * exactly-once. This check fails ONLY on the KAFKA-20808 signature (a fatal committedOffset on a
     * closed/leaked segment), which is always a real defect regardless of recovery. Transient, self-healing
     * conditions (e.g. startup MissingSourceTopicException from stale metadata) are ignored.
     */
    private void checkFatal(final String ctx, final String which, final Throwable t) {
        if (t == null) {
            return;
        }
        final String chain = throwableChain(t).toLowerCase(Locale.ROOT);
        if (chain.contains("committedoffset") || (chain.contains("closed") && chain.contains("segment"))) {
            fail(ctx + ": KAFKA-20808-class fatal on instance " + which + ":\n" + throwableChain(t));
        }
    }

    private String describe(final KafkaStreams ks) {
        return "[state=" + ks.state() + " active=" + safeActive(ks) + " standby=" + safeStandby(ks) + "]";
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
        }, "takeover-producer");
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
        }, "takeover-sink-consumer");
        sinkThread.setDaemon(true);
        sinkThread.start();
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
