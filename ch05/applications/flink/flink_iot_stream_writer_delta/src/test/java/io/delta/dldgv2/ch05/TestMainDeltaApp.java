package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
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

}
