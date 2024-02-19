package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.flink.source.internal.builder.RowDataFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMainDeltaApp {

    @Test
    public void shouldGenerateKafkaSourceAndDeltaSink() throws Exception {
        var app = new FlinkToDeltaSink(new String[]{});
        var kafkaSource = app.getKafkaSource();
        var dataTypeInfo = kafkaSource.getProducedType();
        assertEquals(dataTypeInfo.getTypeClass(), Ecommerce.class);
    }

    @Test
    public void shouldGenerateDeltaSourceAndDeltaSink() throws Exception {
        var app = new DeltaSourceToDeltaSink(new String[]{});
        var deltaBoundedSource = app.getBoundedDeltaSource();
        assertEquals(deltaBoundedSource.getTablePath().toString(), "/opt/data/delta/ecomm_v1_clickstream");

        var deltaContinuousSource = app.getContinuousDeltaSource();
        assertEquals(deltaContinuousSource.getTablePath().toString(), "/opt/data/delta/ecomm_v1_clickstream");
    }

    @Test
    public void shouldGenerateDeltaSourceAndDeltaSinkWithAltConfig() throws AssertionError, Exception {
        var app = new DeltaSourceToDeltaSink(new String[]{ "delta-source-app-w-columns-timestamp.properties" });
        var deltaBoundedSource = app.getBoundedDeltaSource();
        assertEquals(deltaBoundedSource.getTablePath().toString(), "src/test/resources/delta/ecomm_v1_clickstream");

        // simple test to ensure the configuration is being picked up correctly
        if (app.getSourceColumnNames().isPresent()) {
            assertEquals(app.getSourceColumnNames().get().size(), 4);
        } else throw new AssertionError(
                "we expected 4 columns to exist. This configuration must be honored.");

        var producedType = (InternalTypeInfo<RowData>) deltaBoundedSource.getProducedType();
        final var rowType = (RowType) producedType.toLogicalType();
        var fields = rowType.getFields();

        // test that the fields we want actually exist on the source
        var projectedColumnList = app.getSourceColumnNames().orElse(Collections.emptyList());
        var availableFields = fields.stream()
                .filter(f -> projectedColumnList.contains(f.getName())).collect(Collectors.toList());
        assertEquals(availableFields.size(), projectedColumnList.size());

        // simple test to ensure that the continuous source honors the bounded source settings
        var deltaContinuousSource = app.getContinuousDeltaSource();
        assertEquals(deltaContinuousSource.getTablePath().toString(), deltaBoundedSource.getTablePath().toString());
    }

}
