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

import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class Elasticsearch8SinkBuilderTest {

    /**
     * Do not allow invalid Sink objects to be created;
     * The validate method should guarantee that all
     * required properties are in place with valid values
     *
     */
    @Test
    public void sinkBuilderValidate() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> Elasticsearch8SinkBuilder.<String>builder().build());

        assertEquals(null, exception.getMessage());
    }

    /**
     * Emitter should be declared to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetEmitter() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> Elasticsearch8SinkBuilder.<String>builder()
                .setHosts(new HttpHost("localhost", 9200))
                .setPassword("password")
                .setUsername("username")
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The username, if provided, cannot be null to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetUsername() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> Elasticsearch8SinkBuilder.<String>builder()
                .setUsername(null)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The password, if provided, cannot be null to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetPassword() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> Elasticsearch8SinkBuilder.<String>builder()
                .setPassword(null)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }
}
