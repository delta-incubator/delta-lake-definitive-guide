package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.flink.source.internal.builder.RowDataFormat;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    public void shouldGenerateDeltaSourceAndDeltaSinkWithAltConfig() throws Exception {
        var app = new DeltaSourceToDeltaSink(new String[]{ "delta-source-app-w-columns-timestamp.properties" });
        var deltaBoundedSource = app.getBoundedDeltaSource();
        assertEquals(deltaBoundedSource.getTablePath().toString(), "src/test/resources/data/ecomm_v1_clickstream");
        var deltaContinuousSource = app.getContinuousDeltaSource();
        assertEquals(deltaContinuousSource.getTablePath().toString(), deltaBoundedSource.getTablePath().toString());
    }

}
