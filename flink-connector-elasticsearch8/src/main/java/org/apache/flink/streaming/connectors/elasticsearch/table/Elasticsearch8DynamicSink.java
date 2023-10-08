package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSinkBuilder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import org.apache.http.HttpHost;

import java.time.ZoneId;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link Elasticsearch8AsyncSink} from a
 * logical description.
 */
@Internal
public class Elasticsearch8DynamicSink implements DynamicTableSink {

    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final TableSchema schema;
    private final Elasticsearch8Configuration config;
    private final ZoneId localTimeZoneId;
    private final boolean isDynamicIndexWithSystemTime;

    public Elasticsearch8DynamicSink(
            EncodingFormat<SerializationSchema<RowData>> format,
            Elasticsearch8Configuration config,
            TableSchema schema,
            ZoneId localTimeZoneId) {
        this.format = format;
        this.schema = schema;
        this.config = config;
        this.localTimeZoneId = localTimeZoneId;
        this.isDynamicIndexWithSystemTime = isDynamicIndexWithSystemTime();
    }

    public boolean isDynamicIndexWithSystemTime() {
        IndexGeneratorFactory.IndexHelper indexHelper = new IndexGeneratorFactory.IndexHelper();
        return indexHelper.checkIsDynamicIndexWithSystemTimeFormat(config.getIndex());
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        if (isDynamicIndexWithSystemTime && !requestedMode.containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    "Dynamic indexing based on system time only works on append only stream.");
        }
        return builder.build();
    }

    @Override
    public SinkV2Provider getSinkRuntimeProvider(Context context) {
        return () -> {
            final SerializationSchema<RowData> format =
                    this.format.createRuntimeEncoder(context, schema.toRowDataType());

            final IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(
                    config.getIndex(), schema, localTimeZoneId);

            final Function<RowData, String> keyExtractor = KeyExtractor.createKeyExtractor(
                    schema, config.getKeyDelimiter());

            Elasticsearch8AsyncSinkBuilder<RowData> builder = Elasticsearch8AsyncSinkBuilder.builder();
            builder.setHosts(config.getHosts().toArray(new HttpHost[0]));
            builder.setElementConverter(new RowElasticSearchSinkElementConverter(
                    indexGenerator,
                    format,
                    keyExtractor
            ));

            builder.setMaxBatchSize(config.getBulkFlushMaxActions());
            builder.setMaxBatchSizeInBytes(config.getBulkFlushMaxByteSize());
            builder.setMaxTimeInBufferMS(config.getBulkFlushInterval());
            builder.setExceptionWhenSinkFailed(org.apache.commons.lang3.StringUtils.equalsIgnoreCase(config.config.get(FAILURE_HANDLER_OPTION), "FAIL"));

            if (config.getUsername().isPresent()
                    && config.getPassword().isPresent()
                    && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())
                    && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
                builder.setUsername(config.getUsername().get());
                builder.setPassword(config.getPassword().get());
            }

            return builder.build();
        };
    }

    @Override
    public DynamicTableSink copy() {
        return this;
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch8";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Elasticsearch8DynamicSink that = (Elasticsearch8DynamicSink) o;
        return Objects.equals(format, that.format)
                && Objects.equals(schema, that.schema)
                && Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, schema, config);
    }

}
