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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
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
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * FOCUSED, FAST repro for the standby/active recycle-race crash (the "Finding #1" family), narrowed from the
 * long soak ({@code RuntimeChaosLongSoakIntegrationTest.soakKeyValueStandbyRecycle}).
 *
 * <p>The crash is {@code IllegalStateException: Attempted to create an {active|standby} task ... already own}
 * from {@code Tasks.java}'s registry invariants, which kills a StreamThread. The specific variant the soak hit
 * is {@code addStandbyTask} reached via {@code TaskManager.addTasksToStateUpdater} → catch →
 * {@code Tasks.addFailedTask} → {@code addTask}: a task whose state-updater init FAILS is re-added to the
 * registry, but it is already owned (kept as a standby across an overlapping rebalance), so the re-add throws.
 *
 * <p>To hit that path reliably and quickly this repro combines the ingredients the soak showed are needed:
 * <ul>
 *   <li>{@code num.standby.replicas=1}, 1 thread/instance, EOS-v2 + transactional state stores (the KIP-892
 *       path) — so tasks recycle active&harr;standby and a kept standby exists to double-own;</li>
 *   <li>aggressive, role-swapping rebalance config (zero acceptable-recovery-lag, eager warmup, fast probing/
 *       heartbeats) plus shed-to-0 churn (remove an instance's only thread so all its tasks migrate to the
 *       peer, which recycles its warm standbys -> active), alternating instances — so a task is reassigned
 *       mid-recycle; and</li>
 *   <li>injected {@code OFFSET_OUT_OF_RANGE} on the restore consumer — so state-updater init/restore FAILS
 *       during the recycle (this is the trigger the author's proxy-free repro lacks, which is why the
 *       {@code addFailedTask} variant fires reliably here).</li>
 * </ul>
 *
 * <p>Matches the author's {@code StandbyActiveRecycleRaceReproTest} framing: the test FAILS if the crash is
 * observed (bug reproduced) and PASSES otherwise. Both {@code Tasks.java} and {@code TaskManager.java} are
 * identical to {@code origin/trunk}, so a failure here is a real trunk defect.
 */
@Tag("integration")
@Timeout(360)
public class StandbyRecycleRaceEosReproTest {

    private static final String STORE_NAME = "recycle-race-eos-counts";
    private static final int PARTITIONS = 6;
    private static final String[] KEYS = {"k0", "k1", "k2", "k3", "k4", "k5"};
    private static final long CHURN_DURATION_MS = 180_000L;
    private static final String SIGNATURE = "already own"; // catches all four Tasks.java registry invariants

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String inputTopic;
    private String appId;
    private KafkaProducer<String, String> producer;
    private final List<KafkaStreams> instances = new ArrayList<>();

    private final AtomicReference<Throwable> raceCrash = new AtomicReference<>();
    private final AtomicReference<Throwable> otherFatal = new AtomicReference<>();
    private final AtomicInteger raceHits = new AtomicInteger(0);
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private Thread producerThread;

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        appId = "recycle-race-eos-" + safeUniqueTestName(info);
        inputTopic = appId + "-in";
        cluster.createTopic(inputTopic, PARTITIONS, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());

        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()); // direct: input producer never faulted
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
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldNotCrashWithRecycleRaceUnderEosRestoreFaults() throws Exception {
        startProducer();

        final KafkaStreams a = newInstance("instA");
        final KafkaStreams b = newInstance("instB");
        instances.add(a);
        instances.add(b);
        a.start();
        b.start();
        awaitRunning(a, 60_000);
        awaitRunning(b, 60_000);
        Thread.sleep(10_000); // let standbys warm so a standby->active reassignment has real state to recycle

        // Inject the restore-path failure that makes state-updater init fail mid-recycle (-> addFailedTask),
        // plus a migration fault, for the whole churn window.
        proxy.injectError(ApiKeys.FETCH, Errors.OFFSET_OUT_OF_RANGE).forClient("restore").withProbability(0.5);
        proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED).withProbability(0.2);

        // Shed one instance's ONLY thread to 0 so ALL its tasks migrate to the peer, forcing the peer to
        // recycle its warm standbys -> active; then re-add so they migrate back and recycle again. This drives
        // the overlapping standby<->active recycle far harder than adding/removing a second thread (which
        // leaves the base thread's tasks put). Alternating the victim keeps both instances recycling.
        final long deadline = System.currentTimeMillis() + CHURN_DURATION_MS;
        int cycle = 0;
        while (System.currentTimeMillis() < deadline && raceCrash.get() == null) {
            final KafkaStreams victim = (cycle % 2 == 0) ? a : b;
            try {
                victim.removeStreamThread(Duration.ofSeconds(20));
            } catch (final Exception ignore) {
                // may race a rebalance / shutdown; keep churning
            }
            Thread.sleep(4_000); // orphan/recycle window: peer promotes standbys -> active
            try {
                victim.addStreamThread();
            } catch (final Exception ignore) {
                // may race a rebalance; keep churning
            }
            Thread.sleep(3_000);
            cycle++;
        }
        proxy.clearFaults();

        if (raceCrash.get() != null) {
            fail("REPRODUCED recycle-race (Tasks.java '" + SIGNATURE + "' invariant) after " + cycle
                + " churn cycles, hits=" + raceHits.get() + ":\n" + chain(raceCrash.get()));
        }
        System.out.println("RECYCLE-RACE-EOS-REPRO result=NOT_REPRODUCED cycles=" + cycle
            + " otherFatal=" + (otherFatal.get() == null ? "none" : chain(otherFatal.get())));
    }

    private KafkaStreams newInstance(final String clientId) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore(STORE_NAME))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, true); // KIP-892 transactional state stores
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        // Fast, role-swapping rebalances: eager warmup promotion + frequent probing/heartbeats.
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
                raceHits.incrementAndGet();
            } else {
                otherFatal.compareAndSet(null, t);
            }
            // keep churning so the race window stays open (and we can count repeat hits)
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
