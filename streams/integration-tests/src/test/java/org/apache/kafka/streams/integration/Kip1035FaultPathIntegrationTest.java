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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.TaskManager;
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
import java.util.stream.Stream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KIP-1035 (offsets-managed-in-RocksDB) fault-path integration tests. Uses
 * {@link RocksDBStoreCorruptionUtils} to inject on-disk faults (and, in later tests, the wire-level
 * {@code KafkaProtocolFaultProxy}) and asserts the KIP-1035 recovery invariants: an unclean shutdown
 * must be detected and recovered (TaskCorrupted -> wipe -> changelog restore) without a fatal crash,
 * and state must remain exactly-once correct.
 */
@Tag("integration")
@Timeout(300)
public class Kip1035FaultPathIntegrationTest {

    private static final String STORE_NAME = "counts";
    private static final String KEY = "a";

    private EmbeddedKafkaCluster cluster;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private File stateDir;
    private KafkaStreams streams;
    private final java.util.concurrent.atomic.AtomicReference<Throwable> uncaught = new java.util.concurrent.atomic.AtomicReference<>();

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "kip1035-fault-" + base;
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
        return buildAndStart(false);
    }

    private KafkaStreams buildAndStart(final boolean windowed) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        if (windowed) {
            // Windowed count is backed by a segmented store (AbstractRocksDBSegmentedBytesStore) — one
            // RocksDB dir per segment. A single wide window keeps the running count for key "a" in one
            // window so the max observed output value equals the total count.
            builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                .count(Materialized.as(STORE_NAME))
                .toStream()
                .map((wk, v) -> KeyValue.pair(wk.key(), v))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        } else {
            builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as(STORE_NAME))
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        }

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
    public void shouldRecoverExactlyOnceFromUncleanShutdownUnderEos() throws Exception {
        // Phase 1: build state — count for key "a" reaches 5, committed to the store + changelog.
        streams = buildAndStart();
        produce(KEY, 5);
        assertEquals(5L, waitForCount(KEY, 5L), "count should reach 5 before shutdown");
        streams.close(Duration.ofSeconds(30));
        streams = null;

        // Inject the on-disk fault: mark the store's KIP-1035 status key OPEN, i.e. an unclean shutdown.
        final List<File> storeDirs = findStoreDirs(stateDir, STORE_NAME);
        assertTrue(!storeDirs.isEmpty(), "expected to find at least one on-disk store dir for " + STORE_NAME);
        for (final File dir : storeDirs) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(dir);
        }

        // Phase 2: restart. Under EOS the OPEN status must be detected -> TaskCorrupted -> wipe -> restore
        // from the changelog (rebuilding count=5), and the app must recover WITHOUT a fatal crash.
        // Capture TaskManager logs to PROVE the corruption/recovery path was actually taken -- otherwise a
        // clean restart would also reach count=10 + RUNNING and this test would pass hollowly.
        try (final LogCaptureAppender logs = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            streams = buildAndStart();

            // Continue processing: +5 more for "a" must land exactly-once on 10 (restore rebuilt 5, then +5).
            produce(KEY, 5);
            assertEquals(10L, waitForCount(KEY, 10L),
                "count must be exactly-once 10 after unclean-shutdown recovery (restore rebuilt 5, then +5)");

            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "the injected OPEN status must be detected as a corrupted task (proves the recovery path ran)");
        }

        assertNull(uncaught.get(), "recovery from an unclean shutdown must not surface a fatal exception");
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after recovery");
    }

    @Test
    public void shouldRecoverExactlyOnceFromUncleanShutdownForSegmentedStore() throws Exception {
        // Same recovery contract as the KV case, but for a WINDOWED (segmented) store — one RocksDB dir per
        // segment. This is where KAFKA-20808 lived: a segment inserted-before-open surviving closeDirty.
        streams = buildAndStart(true);
        produce(KEY, 5);
        assertEquals(5L, waitForCount(KEY, 5L), "windowed count should reach 5 before shutdown");
        streams.close(Duration.ofSeconds(30));
        streams = null;

        // Mark EVERY segment's KIP-1035 status key OPEN — an unclean shutdown of the segmented store.
        final List<File> segmentDirs = findStoreDirs(stateDir, STORE_NAME);
        assertTrue(segmentDirs.size() >= 1, "expected at least one on-disk segment dir for " + STORE_NAME
            + " (found: " + segmentDirs + ")");
        for (final File dir : segmentDirs) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(dir);
        }

        try (final LogCaptureAppender logs = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            streams = buildAndStart(true);

            produce(KEY, 5);
            assertEquals(10L, waitForCount(KEY, 10L),
                "windowed count must be exactly-once 10 after unclean-shutdown recovery (restore rebuilt 5, then +5)");

            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "the injected OPEN status must be detected as a corrupted task (proves the recovery path ran)");
        }

        assertNull(uncaught.get(), "recovery from an unclean shutdown must not surface a fatal exception");
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after recovery");
    }

    @Test
    public void shouldRecoverExactlyOnceWhenCommittedOffsetsMissingUnderEos() throws Exception {
        // Different branch than status=OPEN: after a CLEAN close the status key is CLOSED (so openDB
        // succeeds), but with the committed offsets deleted, ProcessorStateManager.initializeStoreOffsets
        // sees committedOffset==null on a non-empty store under EOS -> TaskCorrupted -> wipe -> restore.
        streams = buildAndStart();
        produce(KEY, 5);
        assertEquals(5L, waitForCount(KEY, 5L), "count should reach 5 before shutdown");
        streams.close(Duration.ofSeconds(30));
        streams = null;

        final List<File> storeDirs = findStoreDirs(stateDir, STORE_NAME);
        assertTrue(!storeDirs.isEmpty(), "expected to find at least one on-disk store dir for " + STORE_NAME);
        for (final File dir : storeDirs) {
            RocksDBStoreCorruptionUtils.deleteOffsets(dir);
        }

        try (final LogCaptureAppender logs = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            streams = buildAndStart();

            produce(KEY, 5);
            assertEquals(10L, waitForCount(KEY, 10L),
                "count must be exactly-once 10 after missing-offsets recovery (restore rebuilt 5, then +5)");

            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "missing committed offsets on a non-empty EOS store must be treated as corruption (proves recovery ran)");
        }

        assertNull(uncaught.get(), "recovery from missing committed offsets must not surface a fatal exception");
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after recovery");
    }

    // --- helpers ---

    /**
     * Recursively find RocksDB store directories for {@code storeName}, matching both the single-dir
     * KV store ({@code counts}) and the per-segment dirs of a segmented store ({@code counts.<segmentId>}) —
     * i.e. any directory whose name starts with {@code storeName} and contains a RocksDB {@code CURRENT} file.
     */
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

    /** Wait until the output shows the key reach at least {@code min}, then return the max count seen for it. */
    private long waitForCount(final String key, final long min) throws Exception {
        final List<KeyValue<String, Long>> out = waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, 1, 60_000);
        // ensure we've observed the target; the read-committed consumer returns the committed stream of updates
        long max = out.stream().filter(kv -> kv.key.equals(key)).mapToLong(kv -> kv.value).max().orElse(-1);
        final long deadline = System.currentTimeMillis() + 60_000;
        while (max < min && System.currentTimeMillis() < deadline) {
            final List<KeyValue<String, Long>> more = waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, 1, 60_000);
            max = more.stream().filter(kv -> kv.key.equals(key)).mapToLong(kv -> kv.value).max().orElse(max);
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
