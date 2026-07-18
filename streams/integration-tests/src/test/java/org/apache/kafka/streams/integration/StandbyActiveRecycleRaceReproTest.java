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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Focused, PROXY-FREE deterministic-ish repro for the task-lifecycle race that crashes an instance with
 * {@code IllegalStateException: Attempted to create an active task while we already own its standby}
 * ({@code Tasks.addActiveTask}), first surfaced by the runtime-chaos standby rebalance storm.
 *
 * <p>Root idea: with {@code num.standby.replicas=1}, drive RAPID, OVERLAPPING rebalances so a task held as a
 * standby (still in the state updater / pending init) gets reassigned ACTIVE on the same instance before its
 * standby copy is recycled/removed — leaving both representations owned, so the active reaches
 * {@code addActiveTask} while the standby is still present and the instance dies.
 *
 * <p>Rebalance churn is created purely via the public API — repeatedly adding and removing stream threads on
 * both instances in a tight loop — so this test runs unchanged on trunk and on release branches (no fault
 * proxy needed). A background producer keeps changelog lag non-trivial, widening the recycle/restore window.
 *
 * <p>The test FAILS if the invariant crash is observed (repro), and PASSES otherwise (this run didn't hit the
 * race). Run it repeatedly to measure the hit rate and to confirm presence on a given branch.
 */
@Tag("integration")
@Timeout(300)
public class StandbyActiveRecycleRaceReproTest {

    private static final String STORE_NAME = "recycle-race-counts";
    private static final int PARTITIONS = 6;
    private static final String[] KEYS = {"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"};
    private static final long CHURN_DURATION_MS = 120_000L;
    // Common substring of all four Tasks.java registry invariants (active/standby dup + cross-role), so the
    // repro catches the whole recycle-race family: "...create an active task while we already own its standby",
    // "...create an standby task that we already own", "...create an active task that we already own",
    // "...create an standby task while we already own its active".
    private static final String SIGNATURE = "already own";

    private EmbeddedKafkaCluster cluster;
    private String inputTopic;
    private String appId;
    private KafkaProducer<String, String> producer;
    private final List<KafkaStreams> instances = new ArrayList<>();

    private final AtomicReference<Throwable> raceCrash = new AtomicReference<>();
    private final AtomicReference<Throwable> otherFatal = new AtomicReference<>();
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private Thread producerThread;

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        appId = "recycle-race-" + safeUniqueTestName(info);
        inputTopic = appId + "-in";
        cluster.createTopic(inputTopic, PARTITIONS, 1);

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
        for (final KafkaStreams ks : instances) {
            try {
                ks.close(Duration.ofSeconds(20));
            } catch (final Exception ignore) {
                // best effort
            }
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldNotCrashWithActiveWhileStandbyOwnedUnderRapidRebalances() throws Exception {
        startProducer();

        final KafkaStreams a = newInstance("instA");
        final KafkaStreams b = newInstance("instB");
        instances.add(a);
        instances.add(b);
        a.start();
        b.start();
        awaitRunning(a, 60_000);
        awaitRunning(b, 60_000);
        // let standbys warm up so a standby->active reassignment has real state to recycle
        Thread.sleep(10_000);

        // Rapid, overlapping rebalances via thread add/remove on both instances. Each add/remove kicks a
        // rebalance; doing it tight on both overlaps them, so a standby can be reassigned active mid-recycle.
        final long deadline = System.currentTimeMillis() + CHURN_DURATION_MS;
        int cycle = 0;
        while (System.currentTimeMillis() < deadline && raceCrash.get() == null) {
            final KafkaStreams x = (cycle % 2 == 0) ? a : b;
            final KafkaStreams y = (cycle % 2 == 0) ? b : a;
            try {
                x.addStreamThread();
                y.addStreamThread();
                // minimal spacing -> overlapping rebalances
                Thread.sleep(150);
                x.removeStreamThread();
                y.removeStreamThread();
                Thread.sleep(150);
            } catch (final Exception ignore) {
                // add/remove can race with a shutdown-in-progress instance; keep churning
            }
            cycle++;
        }

        if (raceCrash.get() != null) {
            fail("REPRODUCED Finding #1: instance crashed with the standby/active recycle-race invariant:\n"
                + chain(raceCrash.get()));
        }
        // Not a clean pass we care about beyond "did not reproduce this run"; report other fatals for context.
        System.out.println("RECYCLE-RACE-REPRO result=NOT_REPRODUCED cycles=" + cycle
            + " otherFatal=" + (otherFatal.get() == null ? "none" : otherFatal.get().getClass().getSimpleName()));
    }

    private KafkaStreams newInstance(final String clientId) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(STORE_NAME));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        // Fast, role-swapping rebalances: eager warmup promotion + frequent probing.
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0L);
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 12);
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 60_000L);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6_000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 2_000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) t -> {
            if (chain(t).toLowerCase(Locale.ROOT).contains(SIGNATURE)) {
                raceCrash.compareAndSet(null, t);
            } else {
                otherFatal.compareAndSet(null, t);
            }
            // keep the instance churning so the race window stays open
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        return ks;
    }

    private void startProducer() {
        producing.set(true);
        producerThread = new Thread(() -> {
            int i = 0;
            while (producing.get()) {
                producer.send(new ProducerRecord<>(inputTopic, KEYS[i % KEYS.length], "v"));
                i++;
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            producer.flush();
        }, "recycle-race-producer");
        producerThread.setDaemon(true);
        producerThread.start();
    }

    private void awaitRunning(final KafkaStreams ks, final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (ks.state() == KafkaStreams.State.RUNNING) {
                return;
            }
            if (raceCrash.get() != null) {
                return;
            }
            Thread.sleep(200);
        }
    }

    private static String chain(final Throwable t) {
        final StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append('\n');
            cur = cur.getCause();
        }
        return sb.toString();
    }
}
