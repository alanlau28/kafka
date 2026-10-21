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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.JaasUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = SASL_ENABLED_MECHANISMS_CONFIG, value = "PLAIN"),
        @ClusterConfigProperty(key = SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, value = "PLAIN"),
        @ClusterConfigProperty(key = StandardAuthorizer.SUPER_USERS_CONFIG,
            value = "User:" + JaasUtils.KAFKA_PLAIN_ADMIN + ";User:" + JaasUtils.KAFKA_PLAIN_USER1),
        @ClusterConfigProperty(key = ProducerTransientAuthErrorTest.CALLBACK_HANDLER_CONFIG,
            value = "org.apache.kafka.clients.producer.ProducerTransientAuthErrorTest$TogglingPlainServerCallbackHandler"),
    }
)
public class ProducerTransientAuthErrorTest {

    static final String CALLBACK_HANDLER_CONFIG = "listener.name.external.plain.sasl.server.callback.handler.class";

    /** Rejects the test client's user while {@link #REJECT} is set; inter-broker traffic uses a different user. */
    public static class TogglingPlainServerCallbackHandler extends PlainServerCallbackHandler {
        static final AtomicBoolean REJECT = new AtomicBoolean(false);

        @Override
        protected boolean authenticate(String username, char[] password) throws IOException {
            if (REJECT.get() && JaasUtils.KAFKA_PLAIN_USER1.equals(username))
                return false;
            return super.authenticate(username, password);
        }
    }

    private final ClusterInstance cluster;

    public ProducerTransientAuthErrorTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @AfterEach
    public void tearDown() {
        TogglingPlainServerCallbackHandler.REJECT.set(false);
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testSendToNewTopicAfterAuthRecovers() throws Exception {
        cluster.createTopic("hot", 1, (short) 1);
        cluster.createTopic("cold", 1, (short) 1);
        try (Producer<byte[], byte[]> producer = producer(Map.of())) {
            producer.send(new ProducerRecord<>("hot", "v".getBytes())).get();

            transientAuthFailure(producer);
            awaitMetadataUpdateWhileSending(producer, "hot");

            producer.send(new ProducerRecord<>("cold", "v".getBytes())).get();
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    public void testSendFailsFastWhileAuthFails() throws Exception {
        cluster.createTopic("cold", 1, (short) 1);
        TogglingPlainServerCallbackHandler.REJECT.set(true);
        try (Producer<byte[], byte[]> producer = producer(Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000))) {
            long start = System.currentTimeMillis();
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> producer.send(new ProducerRecord<>("cold", "v".getBytes())).get());
            assertInstanceOf(SaslAuthenticationException.class, e.getCause());
            long elapsedMs = System.currentTimeMillis() - start;
            assertTrue(elapsedMs < 30_000, "send should fail fast instead of blocking, took " + elapsedMs + " ms");
        }
    }

    private Producer<byte[], byte[]> producer(Map<String, Object> overrides) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            JaasUtils.KAFKA_PLAIN_USER1, JaasUtils.KAFKA_PLAIN_USER1_PASSWORD));
        configs.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 3000);
        configs.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        // Close idle connections quickly so the producer reconnects while authentication is failing
        configs.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, 1000);
        configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
        configs.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
        configs.putAll(overrides);
        return cluster.producer(configs);
    }

    private static void transientAuthFailure(Producer<byte[], byte[]> producer) throws Exception {
        double failuresBefore = metric(producer, "failed-authentication-total");
        TogglingPlainServerCallbackHandler.REJECT.set(true);
        try {
            TestUtils.waitForCondition(() -> metric(producer, "failed-authentication-total") > failuresBefore,
                15_000, "producer never failed to authenticate");
        } finally {
            TogglingPlainServerCallbackHandler.REJECT.set(false);
        }
        double successesBefore = metric(producer, "successful-authentication-total");
        TestUtils.waitForCondition(() -> metric(producer, "successful-authentication-total") > successesBefore,
            15_000, "producer never authenticated again");
    }

    private static void awaitMetadataUpdateWhileSending(Producer<byte[], byte[]> producer, String topic) throws Exception {
        long since = System.currentTimeMillis();
        TestUtils.waitForCondition(() -> {
            producer.send(new ProducerRecord<>(topic, "v".getBytes())).get();
            return metric(producer, "metadata-age") * 1000 < System.currentTimeMillis() - since;
        }, 15_000, "no successful metadata update after authentication recovered");
    }

    private static double metric(Producer<byte[], byte[]> producer, String name) {
        for (Map.Entry<MetricName, ? extends Metric> e : producer.metrics().entrySet()) {
            if (e.getKey().name().equals(name) && e.getKey().group().equals("producer-metrics"))
                return ((Number) e.getValue().metricValue()).doubleValue();
        }
        throw new IllegalStateException("Metric not found: " + name);
    }
}
