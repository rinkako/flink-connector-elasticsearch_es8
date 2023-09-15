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

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8SinkTest.DummyData;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/** Tests for {@link Elasticsearch8Writer} */
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
    @Timeout(2)
    public void testBulkOnFlushTest() throws IOException, InterruptedException {
        String index = "test-bulk-on-flush";
        int maxBatchSize = 2;

        try(final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
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
    public void testBulkOnBufferTimeFlushTest() throws Exception {
        String index = "test-bulk-on-time-in-buffer";
        int maxBatchSize = 3;

        try (final Elasticsearch8Writer<DummyData> writer = createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);

            assertIdsAreNotWritten(index, new String[]{"test-1", "test-2"});
            context.getTestProcessingTimeService().advance(6000L);

            block();
        }

        assertIdsAreWritten(index, new String[]{"test-1", "test-2"});
    }

    private Elasticsearch8Writer<DummyData> createWriter(
            String index, int maxBatchSize
    ) {
        return new Elasticsearch8Writer<DummyData>(
            new Elasticsearch8SinkBuilder.OperationConverter<>(
                (element, ctx) -> new IndexOperation.Builder<DummyData>()
                    .id(element.getId())
                    .document(element)
                    .index(index)
                    .build(),
                3
            ),
            context,
            maxBatchSize,
            50,
            10_000,
            5 * 1024 * 1024,
            5000, // 5s
            1024 * 1024,
            null,
            null,
            // @TODO get from container
            Collections.singletonList(new HttpHost("0.0.0.0", 9200)),
            Collections.emptyList()
        ) {
            @Override
            protected void submitRequestEntries(List requestEntries, Consumer requestResult) {
                super.submitRequestEntries(requestEntries, (entries) -> {
                    requestResult.accept(entries);
                    unblock();
                });
            }
        };
    }

    private void unblock() {
        lock.lock();
        try {
            completed.signal();
        } finally {
            lock.unlock();
        }
    }

    private void block() throws InterruptedException {
        lock.lock();
        try {
            completed.await();
        } finally {
            lock.unlock();
        }
    }
}
