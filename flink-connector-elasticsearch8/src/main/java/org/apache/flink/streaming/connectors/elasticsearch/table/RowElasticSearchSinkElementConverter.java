package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.function.Function;

/** convertor for convert table row data into es bulk operation variant. **/
@Internal
public class RowElasticSearchSinkElementConverter implements ElementConverter<RowData, BulkOperationVariant> {

    private final IndexGenerator indexGenerator;
    private final SerializationSchema<RowData> serializationSchema;
    private final Function<RowData, String> keyExtractor;

    private final ObjectMapper mapper = new ObjectMapper();

    public RowElasticSearchSinkElementConverter(
            IndexGenerator indexGenerator,
            SerializationSchema<RowData> serializationSchema,
            Function<RowData, String> keyExtractor) {

        this.indexGenerator = Preconditions.checkNotNull(indexGenerator);
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
        this.keyExtractor = Preconditions.checkNotNull(keyExtractor);
    }

    public void open(Sink.InitContext initContext) throws Exception {
        this.serializationSchema.open(new SerializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return initContext.metricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return initContext.getUserCodeClassLoader();
            }
        });
        indexGenerator.open();
    }

    @Override
    public BulkOperationVariant apply(RowData element, SinkWriter.Context context) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                return processUpsert(element, context);
            case UPDATE_BEFORE:
            case DELETE:
                return processDelete(element, context);
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    private BulkOperationVariant processUpsert(RowData element, SinkWriter.Context context) {
        final String key = this.keyExtractor.apply(element);
        final String index = this.indexGenerator.generate(element);
        byte[] document = this.serializationSchema.serialize(element);
        JsonNode jn;
        try {
            jn = this.mapper.readTree(document);
        } catch (IOException e) {
            throw new RuntimeException("cannot parse embedded serializer result to a json node", e);
        }

        IndexOperation.Builder<JsonNode> iob = new IndexOperation.Builder<>();
        iob.index(index);
        iob.id(key);
        iob.document(jn);
        return iob.build();
    }

    private BulkOperationVariant processDelete(RowData element, SinkWriter.Context context) {
        final String key = this.keyExtractor.apply(element);
        final String index = this.indexGenerator.generate(element);
        DeleteOperation.Builder dob = new DeleteOperation.Builder();
        dob.index(index);
        dob.id(key);
        return dob.build();
    }
}
