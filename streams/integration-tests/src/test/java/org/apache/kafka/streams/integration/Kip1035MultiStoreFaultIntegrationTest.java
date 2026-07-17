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
import org.apache.kafka.common.utils.Bytes;
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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.streams.state.KeyValueStore;
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
 * KIP-1035 multi-store, partial-corruption coverage. Two stores in the same task (two aggregations off the
 * same {@code groupByKey}, so no repartition -> one task). Only <b>store A</b> is marked unclean
 * (status=OPEN); store B is left clean. A TaskCorrupted wipe is whole-task, so both stores must be wiped and
 * restored from their changelogs and both must end exactly-once correct — i.e. the recovery neither leaves
 * the clean store B inconsistent nor loses/duplicates its state.
 */
@Tag("integration")
@Timeout(300)
public class Kip1035MultiStoreFaultIntegrationTest {

    private static final String STORE_A = "counts-a";
    private static final String STORE_B = "counts-b";
    private static final String KEY = "a";

    private EmbeddedKafkaCluster cluster;
    private String inputTopic;
    private String outputA;
    private String outputB;
    private String appId;
    private File stateDir;
    private KafkaStreams streams;
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "kip1035-multistore-" + base;
        inputTopic = appId + "-in";
        outputA = appId + "-outA";
        outputB = appId + "-outB";
        cluster.createTopic(inputTopic, 1, 1);
        cluster.createTopic(outputA, 1, 1);
        cluster.createTopic(outputB, 1, 1);
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
        final KGroupedStream<String, String> grouped =
            builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        // Two stores in the same task (same groupByKey, no repartition).
        grouped.count(Materialized.as(STORE_A))
            .toStream().to(outputA, Produced.with(Serdes.String(), Serdes.Long()));
        grouped.aggregate(
                () -> 0L,
                (k, v, agg) -> agg + 1L,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_B).withValueSerde(Serdes.Long()))
            .toStream().to(outputB, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        startApplicationAndWaitUntilRunning(streams);
        return streams;
    }

    @Test
    public void shouldRecoverBothStoresWhenOnlyOneIsCorrupted() throws Exception {
        streams = buildAndStart();
        produce(KEY, 5);
        assertEquals(5L, waitForCount(outputA, KEY, 5L), "store A should reach 5 before shutdown");
        assertEquals(5L, waitForCount(outputB, KEY, 5L), "store B should reach 5 before shutdown");
        streams.close(Duration.ofSeconds(30));
        streams = null;

        // Corrupt ONLY store A; leave store B clean.
        final List<File> aDirs = findStoreDirs(stateDir, STORE_A);
        final List<File> bDirs = findStoreDirs(stateDir, STORE_B);
        assertTrue(!aDirs.isEmpty(), "expected an on-disk dir for " + STORE_A);
        assertTrue(!bDirs.isEmpty(), "expected an on-disk dir for " + STORE_B + " (both stores must exist)");
        for (final File dir : aDirs) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(dir);
        }

        try (final LogCaptureAppender logs = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            streams = buildAndStart();

            produce(KEY, 5);
            // Both stores must be exactly-once 10 after the whole-task wipe+restore — including the clean B.
            assertEquals(10L, waitForCount(outputA, KEY, 10L), "corrupted store A must recover to exactly-once 10");
            assertEquals(10L, waitForCount(outputB, KEY, 10L),
                "clean store B must also recover to exactly-once 10 (not left inconsistent by the partial corruption)");

            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "store A's OPEN status must be detected as corruption (proves the recovery path ran)");
        }

        assertNull(uncaught.get(), "partial (single-store) corruption must recover, not crash the client");
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after recovery");
    }

    // --- helpers ---

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

    private long waitForCount(final String topic, final String key, final long min) throws Exception {
        long max = -1;
        final long deadline = System.currentTimeMillis() + 120_000;
        while (max < min && System.currentTimeMillis() < deadline) {
            final List<KeyValue<String, Long>> out =
                waitUntilMinKeyValueRecordsReceived(consumerConfig(), topic, 1, 60_000);
            max = Math.max(max, out.stream().filter(kv -> kv.key.equals(key)).mapToLong(kv -> kv.value).max().orElse(-1));
        }
        return max;
    }

    private Properties consumerConfig() {
        final Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + appId + "-" + System.nanoTime());
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return c;
    }
}
