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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.internals.RocksDBStoreCorruptionUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KIP-1035 fault-path test for the <b>standby</b> lifecycle — the shape of the reported 4.3.1 crash
 * (KAFKA-20808): a standby task on an instance whose local store was left in an unclean (status=OPEN)
 * state must, on restart/rebalance, be detected as corrupted and recovered (wipe + changelog restore)
 * WITHOUT a fatal {@code InvalidStateStoreException}/{@code SHUTDOWN_CLIENT}, and exactly-once must hold.
 *
 * <p>Two instances share one task (1 input partition + 1 standby replica): one is active, one is standby.
 * We cleanly stop one instance, mark its on-disk store OPEN with {@link RocksDBStoreCorruptionUtils}
 * (simulating an unclean shutdown), and restart it — its task re-initializes against the corrupted store.
 */
@Tag("integration")
@Timeout(300)
public class Kip1035StandbyFaultIntegrationTest {

    private static final String STORE_NAME = "counts";
    private static final String KEY = "a";

    private EmbeddedKafkaCluster cluster;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private File stateDir1;
    private File stateDir2;
    private KafkaStreams streams1;
    private KafkaStreams streams2;
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "kip1035-standby-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        cluster.createTopic(inputTopic, 1, 1);   // 1 partition -> 1 task -> 1 active + 1 standby across the two instances
        cluster.createTopic(outputTopic, 1, 1);
        stateDir1 = TestUtils.tempDirectory();
        stateDir2 = TestUtils.tempDirectory();
    }

    @AfterEach
    public void tearDown() {
        if (streams1 != null) {
            streams1.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    private KafkaStreams buildStreams(final File stateDir) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        return ks;
    }

    @Test
    public void shouldRecoverStandbyFromUncleanShutdownUnderEos() throws Exception {
        // Phase 1: two instances (active + standby for the single task); build state to count("a")=5.
        streams1 = buildStreams(stateDir1);
        streams2 = buildStreams(stateDir2);
        streams1.start();
        streams2.start();
        awaitRunning(streams1, streams2);

        produce(KEY, 5);
        assertEquals(5L, waitForCount(KEY, 5L), "count should reach 5 while both instances run");

        // Phase 2: cleanly stop instance 2, then simulate an unclean shutdown by marking its on-disk store
        // status OPEN. (Clean close writes status=CLOSED; we flip it to OPEN behind Streams' back.)
        streams2.close(Duration.ofSeconds(30));
        final List<File> storeDirs = findStoreDirs(stateDir2, STORE_NAME);
        assertTrue(!storeDirs.isEmpty(), "instance 2 should have an on-disk store dir for " + STORE_NAME
            + " (found: " + storeDirs + ")");
        for (final File dir : storeDirs) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(dir);
        }

        // Phase 3: restart instance 2. Its task re-initializes against the corrupted store; under EOS the
        // OPEN status must be detected as corruption and recovered (wipe + restore) WITHOUT a fatal crash.
        try (final LogCaptureAppender logs = LogCaptureAppender.createAndRegister()) {
            streams2 = buildStreams(stateDir2);
            streams2.start();
            awaitRunning(streams1, streams2);

            // Continue processing: +5 more must land exactly-once on 10 across the recovery/rebalance.
            produce(KEY, 5);
            assertEquals(10L, waitForCount(KEY, 10L),
                "count must be exactly-once 10 after the standby unclean-shutdown recovery");

            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "the injected OPEN status must be detected as a corrupted task (proves the recovery path ran)");
        }

        assertNull(uncaught.get(),
            "a standby unclean-shutdown must recover, not crash the client (this is the KAFKA-20808 shape)");
        assertEquals(KafkaStreams.State.RUNNING, streams1.state(), "instance 1 should be RUNNING");
        assertEquals(KafkaStreams.State.RUNNING, streams2.state(), "instance 2 should be RUNNING after recovery");
    }

    // --- helpers ---

    private void awaitRunning(final KafkaStreams... apps) throws Exception {
        TestUtils.waitForCondition(
            () -> {
                for (final KafkaStreams a : apps) {
                    if (a.state() != KafkaStreams.State.RUNNING) {
                        return false;
                    }
                }
                return true;
            },
            120_000L,
            "expected all instances to reach RUNNING");
    }

    private static List<File> findStoreDirs(final File root, final String storeName) throws Exception {
        final List<File> dirs = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root.toPath())) {
            walk.filter(Files::isDirectory)
                .filter(p -> p.getFileName().toString().startsWith(storeName))
                .filter(p -> Files.exists(p.resolve("CURRENT")))
                .forEach(p -> dirs.add(p.toFile()));
        }
        return dirs;
    }

    private void produce(final String key, final int count) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final List<KeyValue<String, String>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new KeyValue<>(key, "v"));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, records, p, Time.SYSTEM);
    }

    private long waitForCount(final String key, final long min) throws Exception {
        long max = -1;
        final long deadline = System.currentTimeMillis() + 120_000;
        while (max < min && System.currentTimeMillis() < deadline) {
            final List<KeyValue<String, Long>> out =
                waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, 1, 60_000);
            max = Math.max(max, out.stream().filter(kv -> kv.key.equals(key)).mapToLong(kv -> kv.value).max().orElse(-1));
        }
        return max;
    }

    private Properties consumerConfig() {
        final Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + appId);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return c;
    }
}
