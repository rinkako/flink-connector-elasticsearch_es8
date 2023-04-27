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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class Elasticsearch8Writer<InputT> extends AsyncSinkWriter<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8Writer.class);

    private final ElasticsearchAsyncClient esClient;

    public Elasticsearch8Writer(
        ElementConverter<InputT, Operation> elementConverter,
        Sink.InitContext context,
        int maxBatchSize,
        int maxInFlightRequests,
        int maxBufferedRequests,
        long maxBatchSizeInBytes,
        long maxTimeInBufferMS,
        long maxRecordSizeInBytes,
        String username,
        String password,
        HttpHost[] httpHosts,
        Collection<BufferedRequestState<Operation>> state
    ) {
        super(
            elementConverter,
            context,
            AsyncSinkWriterConfiguration.builder()
                .setMaxBatchSize(maxBatchSize)
                .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setMaxBufferedRequests(maxBufferedRequests)
                .setMaxTimeInBufferMS(maxTimeInBufferMS)
                .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                .build(),
            state
        );

        this.esClient = new NetworkConfigFactory(httpHosts, username, password).create();
    }

    @Override
    protected void submitRequestEntries(List<Operation> requestEntries, Consumer<List<Operation>> requestResult) {
        LOG.debug("submitRequestEntries with {} items", requestEntries.size());

        BulkListener<Operation> listener = new BulkListener<Operation>() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request, List<Operation> contexts) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, List<Operation> contexts, BulkResponse response) {
                LOG.debug("Bulk request " + executionId + " completed");
                ArrayList<Operation> retriableEntries = new ArrayList<>();

                for (int i = 0; i < contexts.size(); i++) {
                    BulkResponseItem item = response.items().get(i);
                    Operation operation = contexts.get(i);

                    if (item.error() != null) {
                        LOG.error("Failed operation " + operation + " - " + item.error().reason());
                    }

                    if (item.error() != null && operation.isRetriable()) {
                        retriableEntries.add(operation);
                        operation.retry();
                    }
                }

                requestResult.accept(retriableEntries);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, List<Operation> contexts, Throwable failure) {
                LOG.debug("Bulk request " + executionId + " failed", failure);
                requestResult.accept(requestEntries);
            }
        };

        try (BulkIngester<Operation> ingester = BulkIngester.of(b -> b
            .client(esClient)
            .listener(listener)
        )) {
            for (Operation operation : requestEntries) {
                ingester.add(new BulkOperation(operation.getBulkOperationVariant()), operation);
            }
        }
    }

    @Override
    protected long getSizeInBytes(Operation requestEntry) {
        return new OperationSerializer().size(requestEntry);
    }
}
