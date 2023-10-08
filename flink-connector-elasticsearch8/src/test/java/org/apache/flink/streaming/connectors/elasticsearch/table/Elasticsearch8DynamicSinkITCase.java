/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TestLogger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;
import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** IT tests for {@link Elasticsearch8DynamicSink}. */
public class Elasticsearch8DynamicSinkITCase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8DynamicSinkITCase.class);

    public static final String ELASTICSEARCH_VERSION = "8.9.1";

    public static final DockerImageName ELASTICSEARCH_IMAGE = DockerImageName
            .parse("docker.elastic.co/elasticsearch/elasticsearch")
            .withTag(ELASTICSEARCH_VERSION);

    @Container
    public static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();

    public static final String MERGED_HOST = String.format("http://%s:%s", ES_CONTAINER.getHost(), ES_CONTAINER.getFirstMappedPort());

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

    public RestClient client;

    public ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        this.client = getRestClient();
    }

    @AfterEach
    void shutdown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    public RestClient getRestClient() {
        return RestClient.builder(new HttpHost(ES_CONTAINER.getHost(), ES_CONTAINER.getFirstMappedPort()))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder)
                .build();
    }

    public List<JsonNode> fetchByIndex(String index) throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());
        assertEquals(200, response.getStatusLine().getStatusCode());
        List<JsonNode> res = new LinkedList<>();
        JsonNode respTree = objectMapper.readTree(responseEntity);
        ArrayNode hits = (ArrayNode) respTree.get("hits").get("hits");
        hits.forEach(res::add);
        return res;
    }

    public List<JsonNode> assertIdsAreWritten(String index, String[] ids) throws IOException {
        List<JsonNode> hits = this.fetchByIndex(index);

        Set<String> idSet = new HashSet<>();
        hits.forEach((t) -> {
            idSet.add(t.get("_id").asText());
        });

        for (String id : ids) {
            assertTrue("assert check id:" + id, idSet.contains(id));
        }

        return hits;
    }

    public void deleteByIndex(String index) throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("DELETE", index));
        String responseEntity = EntityUtils.toString(response.getEntity());
        assertEquals(200, response.getStatusLine().getStatusCode());
        JsonNode respTree = objectMapper.readTree(responseEntity);
        assertTrue(respTree.get("acknowledged").asBoolean());
    }

    @Test
    public void testWritingDocuments() throws Exception {
        ResolvedSchema schema = new ResolvedSchema(Arrays.asList(Column.physical(
                        "a",
                        DataTypes.BIGINT().notNull()),
                Column.physical("b", DataTypes.TIME()),
                Column.physical("c", DataTypes.STRING().notNull()),
                Column.physical("d", DataTypes.FLOAT()),
                Column.physical("e", DataTypes.TINYINT().notNull()),
                Column.physical("f", DataTypes.DATE()),
                Column.physical("g", DataTypes.TIMESTAMP().notNull())),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("name", Arrays.asList("a", "g")));

        GenericRowData rowData = GenericRowData.of(1L,
                12345,
                StringData.fromString("ABCDE"),
                12.12f,
                (byte) 2,
                12345,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12")));

        final String index = "es8-dynamic-tests-writing-documents";
        Elasticsearch8DynamicSinkFactory sinkFactory = new Elasticsearch8DynamicSinkFactory();

        SinkV2Provider sinkRuntimeProvider = (SinkV2Provider) sinkFactory
                .createDynamicTableSink(context()
                        .withSchema(schema)
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                        .withOption(ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                MERGED_HOST)
                        .withOption(
                                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                                "false")
                        .build())
                .getSinkRuntimeProvider(new MockContext());

        Sink<RowData> sinkFunction = sinkRuntimeProvider.createSink();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(100);
        environment.setParallelism(1);

        rowData.setRowKind(RowKind.UPDATE_AFTER);
        environment.<RowData>fromElements(rowData).sinkTo(sinkFunction);
        environment.execute();

        List<JsonNode> results = this.assertIdsAreWritten(index,
                new String[]{"1_2012-12-12T12:12:12"});
        assertEquals(1, results.size());
        JsonNode singleton = results.get(0).get("_source");
        HashMap fbRes = this.objectMapper.convertValue(singleton, HashMap.class);

        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(fbRes).isEqualTo(expectedMap);

        // erase for next time unit test
        deleteByIndex(index);
    }

    @Test
    public void testWritingDocumentsFromTableApi() throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final String index = "es8-dynamic-tests-table-api";
        tableEnvironment.executeSql("CREATE TABLE esTable (" + "a BIGINT NOT NULL,\n" + "b TIME,\n"
                + "c STRING NOT NULL,\n" + "d FLOAT,\n" + "e TINYINT NOT NULL,\n" + "f DATE,\n"
                + "g TIMESTAMP NOT NULL," + "h as a + 2,\n" + "PRIMARY KEY (a, g) NOT ENFORCED\n"
                + ")\n" + "WITH (\n" + String.format("'%s'='%s',\n", "connector", "elasticsearch-8")
                + String.format("'%s'='%s',\n",
                ElasticsearchConnectorOptions.INDEX_OPTION.key(),
                index) + String.format("'%s'='%s',\n",
                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                MERGED_HOST) + String.format("'%s'='%s'\n",
                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                "false") + ")");

        tableEnvironment.fromValues(row(1L,
                LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                "ABCDE",
                12.12f,
                (byte) 2,
                LocalDate.ofEpochDay(12345),
                LocalDateTime.parse("2012-12-12T12:12:12"))).executeInsert("esTable").await();

        List<JsonNode> results = this.assertIdsAreWritten(index,
                new String[]{"1_2012-12-12T12:12:12"});
        assertEquals(1, results.size());
        JsonNode singleton = results.get(0).get("_source");
        HashMap fbRes = this.objectMapper.convertValue(singleton, HashMap.class);

        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(fbRes).isEqualTo(expectedMap);

        // erase for next time unit test
        deleteByIndex(index);
    }

    @Test
    public void testWritingDocumentsNoPrimaryKey() throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final String index = "es8-dynamic-tests-no-primary-key";
        tableEnvironment.executeSql("CREATE TABLE esTable (" + "a BIGINT NOT NULL,\n" + "b TIME,\n"
                + "c STRING NOT NULL,\n" + "d FLOAT,\n" + "e TINYINT NOT NULL,\n" + "f DATE,\n"
                + "g TIMESTAMP NOT NULL\n" + ")\n" + "WITH (\n" + String.format(
                "'%s'='%s',\n",
                "connector",
                "elasticsearch-8") + String.format("'%s'='%s',\n",
                ElasticsearchConnectorOptions.INDEX_OPTION.key(),
                index) + String.format("'%s'='%s',\n",
                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                MERGED_HOST) + String.format("'%s'='%s'\n",
                ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                "false") + ")");;

        tableEnvironment
                .fromValues(row(1L,
                        LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                        "ABCDE",
                        12.12f,
                        (byte) 2,
                        LocalDate.ofEpochDay(12345),
                        LocalDateTime.parse("2012-12-12T12:12:12")), row(2L,
                        LocalTime.ofNanoOfDay(12345L * 1_000_000L),
                        "FGHIJK",
                        13.13f,
                        (byte) 4,
                        LocalDate.ofEpochDay(12345),
                        LocalDateTime.parse("2013-12-12T13:13:13")))
                .executeInsert("esTable")
                .await();

        List<JsonNode> results = this.fetchByIndex(index);
        assertEquals(2, results.size());

        HashSet<Map<String, Object>> resultSet = new HashSet<>();
        JsonNode m0 = results.get(0).get("_source");
        HashMap res0 = this.objectMapper.convertValue(m0, HashMap.class);
        JsonNode m1 = results.get(1).get("_source");
        HashMap res1 = this.objectMapper.convertValue(m1, HashMap.class);
        resultSet.add(res0);
        resultSet.add(res1);
        Map<Object, Object> expectedMap1 = new HashMap<>();
        expectedMap1.put("a", 1);
        expectedMap1.put("b", "00:00:12");
        expectedMap1.put("c", "ABCDE");
        expectedMap1.put("d", 12.12d);
        expectedMap1.put("e", 2);
        expectedMap1.put("f", "2003-10-20");
        expectedMap1.put("g", "2012-12-12 12:12:12");
        Map<Object, Object> expectedMap2 = new HashMap<>();
        expectedMap2.put("a", 2);
        expectedMap2.put("b", "00:00:12");
        expectedMap2.put("c", "FGHIJK");
        expectedMap2.put("d", 13.13d);
        expectedMap2.put("e", 4);
        expectedMap2.put("f", "2003-10-20");
        expectedMap2.put("g", "2013-12-12 13:13:13");
        HashSet<Map<Object, Object>> expectedSet = new HashSet<>();
        expectedSet.add(expectedMap1);
        expectedSet.add(expectedMap2);
        assertThat(resultSet).isEqualTo(expectedSet);

        // erase for next time unit test
        deleteByIndex(index);
    }

    @Test
    public void testWritingDocumentsWithDynamicIndex() throws Exception {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final String indexPtn = "es8-dynamic-tests-dynamic-index-{b|yyyy-MM-dd}";
        final String indexRes = "es8-dynamic-tests-dynamic-index-2012-12-12";
        tableEnvironment.executeSql(
                "CREATE TABLE esTable (" + "a BIGINT NOT NULL,\n" + "b TIMESTAMP NOT NULL,\n"
                        + "PRIMARY KEY (a) NOT ENFORCED\n" + ")\n" + "WITH (\n" + String.format(
                        "'%s'='%s',\n",
                        "connector",
                        "elasticsearch-8") + String.format("'%s'='%s',\n",
                        ElasticsearchConnectorOptions.INDEX_OPTION.key(),
                        indexPtn) + String.format("'%s'='%s',\n",
                        ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                        MERGED_HOST) + String.format("'%s'='%s'\n",
                        ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION.key(),
                        "false") + ")");;

        tableEnvironment
                .fromValues(row(1L, LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("esTable")
                .await();

        List<JsonNode> results = this.fetchByIndex(indexRes);
        assertEquals(1, results.size());
        JsonNode singleton = results.get(0).get("_source");
        HashMap fbRes = this.objectMapper.convertValue(singleton, HashMap.class);
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "2012-12-12 12:12:12");
        assertThat(fbRes).isEqualTo(expectedMap);

        // erase for next time unit test
        deleteByIndex(indexRes);
    }

    private static class MockContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public TypeInformation<?> createTypeInformation(LogicalType consumedLogicalType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }
    }
}
