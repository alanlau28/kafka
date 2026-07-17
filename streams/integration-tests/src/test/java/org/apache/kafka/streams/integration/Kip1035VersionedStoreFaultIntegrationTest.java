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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.streams.state.Stores;
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

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KIP-1035 fault-path test for the <b>versioned</b> store family ({@code RocksDBVersionedStore}, backed by
 * one physical RocksDB store with an offsets column family — it participates in KIP-1035 offset management
 * via {@code managesOffsets()}/{@code committedOffset()}). Simulates an unclean shutdown (status=OPEN) and
 * asserts the same recovery contract as the other families: detected as corrupted, wiped and restored from
 * the changelog, no fatal crash, and the latest value preserved.
 */
@Tag("integration")
@Timeout(300)
public class Kip1035VersionedStoreFaultIntegrationTest {

    private static final String STORE_NAME = "versioned-counts";
    private static final String KEY = "a";

    private EmbeddedKafkaCluster cluster;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private File stateDir;
    private KafkaStreams streams;
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "kip1035-versioned-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        cluster.createTopic(inputTopic, 1, 1);
        cluster.createTopic(outputTopic, 1, 1);
        stateDir = TestUtils.tempDirectory();
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    private KafkaStreams buildAndStart() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as(Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofDays(1))))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        startApplicationAndWaitUntilRunning(ks);
        return ks;
    }

    @Test
    public void shouldRecoverFromUncleanShutdownForVersionedStore() throws Exception {
        // Phase 1: latest value for key "a" advances to 5.
        streams = buildAndStart();
        produceIncreasing(KEY, 1, 5);
        assertEquals(5L, waitForLatest(KEY, 5L), "latest value should reach 5 before shutdown");
        streams.close(Duration.ofSeconds(30));
        streams = null;

        // Mark the versioned store's (single physical) RocksDB status key OPEN -> unclean shutdown.
        final List<File> storeDirs = findRocksDbDirs(stateDir);
        assertTrue(!storeDirs.isEmpty(), "expected an on-disk RocksDB dir for the versioned store (found: " + storeDirs + ")");
        for (final File dir : storeDirs) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(dir);
        }

        // Phase 2: restart; must detect corruption, wipe, restore from changelog, and recover.
        try (final LogCaptureAppender logs = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            streams = buildAndStart();

            produceIncreasing(KEY, 6, 10);
            assertEquals(10L, waitForLatest(KEY, 10L),
                "latest value must be 10 after versioned-store unclean-shutdown recovery (restore rebuilt 5, then to 10)");

            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "the injected OPEN status must be detected as a corrupted task (proves the recovery path ran)");
        }

        assertNull(uncaught.get(), "versioned-store recovery from an unclean shutdown must not surface a fatal exception");
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after recovery");
    }

    // --- helpers ---

    /** Any RocksDB directory (contains a CURRENT file) under the state dir — the versioned store's physical dir. */
    private static List<File> findRocksDbDirs(final File root) throws Exception {
        final List<File> dirs = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root.toPath())) {
            walk.filter(Files::isDirectory)
                .filter(p -> Files.exists(p.resolve("CURRENT")))
                .forEach(p -> dirs.add(p.toFile()));
        }
        return dirs;
    }

    /** Produce values {@code from..to} (inclusive, as strings) for {@code key}, advancing the latest value. */
    private void produceIncreasing(final String key, final int from, final int to) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final List<KeyValue<String, String>> records = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            records.add(new KeyValue<>(key, Integer.toString(i)));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, records, p, Time.SYSTEM);
    }

    /** Wait until the latest observed (committed) value for {@code key} reaches {@code min}; return the max seen. */
    private long waitForLatest(final String key, final long min) throws Exception {
        long max = -1;
        final long deadline = System.currentTimeMillis() + 120_000;
        while (max < min && System.currentTimeMillis() < deadline) {
            final List<KeyValue<String, String>> out =
                waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, 1, 60_000);
            max = Math.max(max, out.stream()
                .filter(kv -> kv.key.equals(key))
                .mapToLong(kv -> Long.parseLong(kv.value))
                .max().orElse(-1));
        }
        return max;
    }

    private Properties consumerConfig() {
        final Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + appId);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return c;
    }
}
