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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

/** Base Integration tests class. */
@Testcontainers
public class ElasticsearchSinkBaseITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkBaseITCase.class);

    public RestClient client;

    public ElasticsearchClient esClient;

    public static final String ELASTICSEARCH_VERSION = "8.9.1";

    public static final DockerImageName ELASTICSEARCH_IMAGE = DockerImageName
        .parse("docker.elastic.co/elasticsearch/elasticsearch")
        .withTag(ELASTICSEARCH_VERSION);

    @Container
    public static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();

    public static ElasticsearchContainer createElasticsearchContainer() {
        try (ElasticsearchContainer container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
            .withEnv("xpack.security.enabled", "false");
        ) {
            container
                .withEnv("ES_JAVA_OPTS", "-Xms2g -Xmx2g")
                .withEnv("logger.org.elasticsearch", "DEBUG")
                .withLogConsumer(new Slf4jLogConsumer(LOG));

            return container;
        }
    }

    public RestClient getRestClient() {
        return RestClient.builder(new HttpHost(ES_CONTAINER.getHost(), ES_CONTAINER.getFirstMappedPort()))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder)
            .build();
    }

    public void assertIdsAreWritten(String index, String[] ids) throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());

        assertEquals(200, response.getStatusLine().getStatusCode());

        for (String id : ids) {
            System.out.println(id);
            assertThat(responseEntity).contains(id);
        }
    }

    public void assertIdsAreNotWritten(String index, String[] ids) throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());

        assertEquals(200, response.getStatusLine().getStatusCode());

        for (String id : ids) {
            assertThat(responseEntity).doesNotContain(id);
        }
    }
}
