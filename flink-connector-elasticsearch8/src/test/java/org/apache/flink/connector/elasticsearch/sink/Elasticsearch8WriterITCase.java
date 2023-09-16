/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8SinkTest.DummyData;
import org.apache.flink.metrics.Gauge;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link Elasticsearch8Writer}. */
@Testcontainers
public class Elasticsearch8WriterITCase extends ElasticsearchSinkBaseITCase {
    private TestSinkInitContext context;

    private final Lock lock = new ReentrantLock();

    private final Condition completed = lock.newCondition();

    @BeforeEach
    void setUp() {
        this.context = new TestSinkInitContext();
        this.client = getRestClient();
    }

    @AfterEach
    void shutdown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @Timeout(5)
    public void testBulkOnFlush() throws IOException, InterruptedException {
        String index = "test-bulk-on-flush";
        int maxBatchSize = 2;

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);

            writer.flush(false);
            assertIdsAreWritten(index, new String[]{"test-1", "test-2"});

            writer.write(new DummyData("3", "test-3"), null);

            writer.flush(true);
            assertIdsAreWritten(index, new String[]{"test-3"});
        }
    }

    @Test
    @Timeout(5)
    public void testBulkOnBufferTimeFlush() throws Exception {
        String index = "test-bulk-on-time-in-buffer";
        int maxBatchSize = 3;

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);

            assertIdsAreNotWritten(index, new String[]{"test-1", "test-2"});
            context.getTestProcessingTimeService().advance(6000L);

            await();
        }

        assertIdsAreWritten(index, new String[]{"test-1", "test-2"});
    }

    @Test
    @Timeout(5)
    public void testBytesSentMetric() throws Exception {
        String index = "test-bytes-sent-metrics";
        int maxBatchSize = 3;

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
            assertThat(context.getNumBytesOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumBytesOutCounter().getCount()).isGreaterThan(0);
        assertIdsAreWritten(index, new String[]{"test-1", "test-2", "test-3"});
    }

    @Test
    @Timeout(5)
    public void testRecordsSentMetric() throws Exception {
        String index = "test-records-sent-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
            assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(3);
        assertIdsAreWritten(index, new String[]{"test-1", "test-2", "test-3"});
    }

    @Test
    @Timeout(5)
    public void testSendTimeMetric() throws Exception {
        String index = "test-send-time-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
            final Optional<Gauge<Long>> currentSendTime = context.getCurrentSendTimeGauge();

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }

        assertIdsAreWritten(index, new String[]{"test-1", "test-2", "test-3"});
    }

    @Test
    @Timeout(5)
    public void testHandlePartiallyFailedBulk() throws Exception {
        String index = "test-partially-failed-bulk";
        int maxBatchSize = 2;

        Elasticsearch8SinkBuilder.OperationConverter<DummyData> elementConverter = new Elasticsearch8SinkBuilder.OperationConverter<>(
            (element, ctx) -> new UpdateOperation.Builder<>()
                .id(element.getId())
                .index(index)
                .action(ac -> ac.doc(element).docAsUpsert(element.getId().equals("test-2")))
                .build(),
            3
        );

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize, elementConverter)) {
            writer.write(new DummyData("test-1", "test-1-updated"), null);
            writer.write(new DummyData("test-2", "test-2-updated"), null);
        }

        await();

        assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(1);
        assertIdsAreWritten(index, new String[]{"test-2"});
        assertIdsAreNotWritten(index, new String[]{"test-1"});
    }

    private Elasticsearch8SinkBuilder.OperationConverter<DummyData> getDefaultTestElementConverter(String index) {
        return new Elasticsearch8SinkBuilder.OperationConverter<>(
            (element, ctx) -> new IndexOperation.Builder<DummyData>()
                .id(element.getId())
                .document(element)
                .index(index)
                .build(),
            3
        );
    }

    private Elasticsearch8Writer<DummyData> createWriter(
        String index,
        int maxBatchSize
    ) {
        return createWriter(index, maxBatchSize, getDefaultTestElementConverter(index));
    }

    private Elasticsearch8Writer<DummyData> createWriter(
        String index,
        int maxBatchSize,
        Elasticsearch8SinkBuilder.OperationConverter<DummyData> elementConverter
    ) {
        return new Elasticsearch8Writer<DummyData>(
            elementConverter,
            context,
            maxBatchSize,
            50,
            10_000,
            5 * 1024 * 1024,
            5000, // 5s
            1024 * 1024,
            null,
            null,
            Collections.singletonList(new HttpHost(ES_CONTAINER.getHost(), 9400)),
            Collections.emptyList()
        ) {
            @Override
            protected void submitRequestEntries(
                List<Operation> requestEntries,
                Consumer<List<Operation>> requestResult)
            {
                super.submitRequestEntries(
                    requestEntries,
                    (entries) -> {
                        requestResult.accept(entries);

                        lock.lock();
                        try {
                            completed.signal();
                        } finally {
                            lock.unlock();
                        }
                    });
            }
        };
    }

    private void await() throws InterruptedException {
        lock.lock();
        try {
            completed.await();
        } finally {
            lock.unlock();
        }
    }
}

