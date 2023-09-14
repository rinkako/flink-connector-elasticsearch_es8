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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import org.apache.http.HttpHost;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Elasticsearch8SinkBuilder
 * The builder to construct the Elasticsearch8Sink {@link Elasticsearch8Sink}.
 *
 * @param <InputT> the type of records to be sunk into an Elasticsearch cluster
 */
public class Elasticsearch8SinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, Operation, Elasticsearch8SinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1024 * 1024;

    /** The hosts where the Elasticsearch cluster is reachable. */
    private HttpHost[] hosts;

    /** The username to authenticate the connection with the Elasticsearch cluster. */
    private String username;

    /** The password to authenticate the connection with the Elasticsearch cluster. */
    private String password;

    /** The element converter that will be called on every stream element to be processed and buffered. */
    private ElementConverter<InputT, BulkOperationVariant> elementConverter;

    /** The number of times an Operation will be retried. */
    private int maxRetries;

    /**
     * setHosts
     * set the hosts where the Elasticsearch cluster is reachable.
     *
     * @param hosts the hosts address
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkArgument(hosts.length > 0, "Hosts cannot be empty");
        this.hosts = hosts;
        return this;
    }

    /**
     * setUsername
     * set the username to authenticate the connection with the Elasticsearch cluster.
     *
     * @param username the auth username
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setUsername(String username) {
        checkNotNull(username, "Username must not be null");
        this.username = username;
        return this;
    }

    /**
     * setPassword
     * set the password to authenticate the connection with the Elasticsearch cluster.
     *
     * @param password the auth password
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setPassword(String password) {
        checkNotNull(password, "Password must not be null");
        this.password = password;
        return this;
    }

    /**
     * setElementConverter
     * set the element converter that will be called at every stream element to be processed and buffered.
     *
     * @param elementConverter elementConverter operation
     * @return {@code ElasticsearchSinkBuilder}
     */
    public Elasticsearch8SinkBuilder<InputT> setElementConverter(
        ElementConverter<InputT, BulkOperationVariant> elementConverter
    ) {
        checkNotNull(elementConverter);
        this.elementConverter = elementConverter;
        return this;
    }

    /**
     * setMaxRetries
     * set the number of times an operation will be retried.
     *
     * @param maxRetries AtomicInteger
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
     * Creates an ElasticsearchSink instance.
     *
     * @return {@link Elasticsearch8Sink}
     */
    @Override
    public Elasticsearch8Sink<InputT> build() {
        return new Elasticsearch8Sink<>(
            buildOperationConverter(elementConverter, maxRetries),
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

    private OperationConverter<InputT> buildOperationConverter(
        ElementConverter<InputT, BulkOperationVariant> converter,
        int maxRetries
    ) {
        return converter != null
            ? new OperationConverter<>(converter, maxRetries)
            : null;
    }

    /** A wrapper that evolves the Operation, since a BulkOperationVariant is not Serializable. */
    public static class OperationConverter<T> implements ElementConverter<T, Operation> {
        private final ElementConverter<T, BulkOperationVariant> converter;

        private final AtomicInteger maxRetries;

        public OperationConverter(ElementConverter<T, BulkOperationVariant> converter, int maxRetries) {
            this.converter = converter;
            this.maxRetries = new AtomicInteger(maxRetries);
        }

        @Override
        public Operation apply(T element, SinkWriter.Context context) {
            return new Operation(converter.apply(element, context), maxRetries);
        }
    }
}

