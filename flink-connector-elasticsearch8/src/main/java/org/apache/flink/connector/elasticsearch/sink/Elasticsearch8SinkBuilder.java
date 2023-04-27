package org.apache.flink.connector.elasticsearch.sink;

/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.http.HttpHost;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class Elasticsearch8SinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, Operation, Elasticsearch8SinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1024 * 1024;

    /**
     * The hosts where the Elasticsearch cluster is reachable
     *
     */
    private HttpHost[] hosts;

    /**
     * The username to authenticate the connection with the Elasticsearch cluster
     *
     */
    private String username;

    /**
     * The password to authenticate the connection with the Elasticsearch cluster
     *
     */
    private String password;

    /**
     * The converter that will be called on every stream element to be processed and buffered
     *
     */
    private ElementConverter<InputT, BulkOperationVariant> converter;

    /**
     * The number of times that an operation will be retried
     *
     */
    private int maxRetries;

    /**
     * setHost
     * set the host where the Elasticsearch cluster is reachable
     *
     * @param hosts the hosts address
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty");
        this.hosts = hosts;
        return this;
    }

    /**
     * setUsername
     * set the username to authenticate the connection with the Elasticsearch cluster
     *
     * @param username the auth username
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }

    /**
     * setPassword
     * set the password to authenticate the connection with the Elasticsearch cluster
     *
     * @param password the auth password
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }

    /**
     * setConverter
     * set the converter that will be called at every stream element to be processed and buffered
     *
     * @param converter converter operation
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setConverter(
        ElementConverter<InputT, BulkOperationVariant> converter
    ) {
        checkNotNull(converter);
        this.converter = converter;
        return this;
    }

    /**
     * setMaxRetries
     * set the number of times an operation will be retried
     *
     * @param maxRetries number
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setMaxRetries(
        int maxRetries
    ) {
        checkState(maxRetries > 0, "The retry number should be greater than zero");
        this.maxRetries = maxRetries;
        return this;
    }

    public static <T> Elasticsearch8SinkBuilder<T> builder() {
        return new Elasticsearch8SinkBuilder<>();
    }

    /**
     * Creates an ElasticsearchSink instance
     *
     * @return ElasticsearchSink
     */
    @Override
    public Elasticsearch8Sink<InputT> build() {
        return new Elasticsearch8Sink<>(
            new OperationConverter<>(converter, maxRetries),
            Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
            Optional.ofNullable(getMaxInFlightRequests()).orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
            Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
            Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
            Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
            Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
            username,
            password,
            hosts
        );
    }

    private static class OperationConverter<T> implements ElementConverter<T, Operation> {
        private final ElementConverter<T, BulkOperationVariant> converter;

        private final int maxRetries;

        public OperationConverter(ElementConverter<T, BulkOperationVariant> converter, int maxRetries) {
            this.converter = converter;
            this.maxRetries = maxRetries;
        }

        @Override
        public Operation apply(T element, SinkWriter.Context context) {
            return new Operation(converter.apply(element, context), maxRetries);
        }
    }
}

