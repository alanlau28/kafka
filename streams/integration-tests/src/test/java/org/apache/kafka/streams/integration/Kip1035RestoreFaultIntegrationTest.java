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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.FaultRule;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KIP-1035 restore-path fault test using the wire proxy. Wipes local state to force a full changelog
 * restore, then injects {@code OFFSET_OUT_OF_RANGE} on the <b>restore consumer's</b> fetch (scoped via
 * {@code forClient("restore")}, so the main consumer is untouched) — the KAFKA-20805 symptom. The restore
 * must survive the out-of-range fetch (reset + continue), recover to RUNNING, and keep state exactly-once.
 */
@Tag("integration")
@Timeout(300)
public class Kip1035RestoreFaultIntegrationTest {

    private static final String STORE_NAME = "counts";
    private static final String KEY = "a";

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private KafkaStreams streams;
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "kip1035-restore-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        cluster.createTopic(inputTopic, 1, 1);
        cluster.createTopic(outputTopic, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    private KafkaStreams buildAndStart() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers()); // route through the proxy
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
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
    public void shouldRecoverWhenRestoreConsumerHitsOffsetOutOfRange() throws Exception {
        // Phase 1: build state (count "a" = 5) into the store + changelog.
        streams = buildAndStart();
        produce(KEY, 5);
        assertEquals(5L, waitForCount(KEY, 5L), "count should reach 5 before restore");

        // Wipe local state so the restart must fully restore from the changelog (restore consumer fetches).
        streams.close(Duration.ofSeconds(30));
        streams.cleanUp();
        streams = null;

        // Phase 2: fault the RESTORE consumer's first two fetches with OFFSET_OUT_OF_RANGE. Scoped to the
        // restore client, so the main consumer/producer are untouched. Bounded (times(2)) so restore can
        // reset and complete rather than looping forever.
        final FaultRule oor = proxy.injectError(ApiKeys.FETCH, Errors.OFFSET_OUT_OF_RANGE)
            .forClient("restore")
            .times(2);

        streams = buildAndStart(); // restores from the changelog through the faulted fetches

        // Continue processing: +5 must land exactly-once on 10 (restore rebuilt 5 despite the OOR, then +5).
        produce(KEY, 5);
        assertEquals(10L, waitForCount(KEY, 10L),
            "count must be exactly-once 10 after a restore that hit OFFSET_OUT_OF_RANGE");

        assertTrue(oor.timesTriggered() >= 1,
            "the restore consumer's fetch should have been faulted with OFFSET_OUT_OF_RANGE (fired="
                + oor.timesTriggered() + ")");
        assertNull(uncaught.get(), "a transient OFFSET_OUT_OF_RANGE during restore must not crash the client");
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should be RUNNING after restore");
    }

    // --- helpers ---

    private void produce(final String key, final int count) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()); // producer talks to broker directly
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
