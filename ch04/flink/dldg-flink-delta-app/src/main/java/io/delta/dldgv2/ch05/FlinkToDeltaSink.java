package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlinkToDeltaSink extends DeltaFlinkBase {
    final static String defaultTableName = "ecomm_v1_clickstream";
    final static String defaultDeltaRootDir = "/tmp/delta";
    final static String defaultCheckpointDir = "/tmp/checkpoints/";
    public static Logger logger = LoggerFactory.getLogger(FlinkToDeltaSink.class);

    public FlinkToDeltaSink(final String[] args) throws IOException {
        super(args);
    }

    public static void main(final String[] args) throws Exception {
        new FlinkToDeltaSink(args).run();
    }

    /**
     * Sets up the Flink application to read from a Kafka topic
     *
     * @return The KafkaSource instance
     * @link {<a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/">docs</a>}
     */
    public KafkaSource<Ecommerce> getKafkaSource() {
        // Fetch configs for Kafka
        var brokers = this.appProps.get("kafka.brokers", "127.0.0.1:29092");
        var groupId = this.appProps.get("kafka.group.id", "delta-dldg-1");
        var topic = this.appProps.get("kafka.topic", "ecomm.v1.clickstream");

        return KafkaSource.<Ecommerce>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(Ecommerce.jsonFormat)
                .build();
    }

    public DataStreamSink<RowData> createDataStream(final StreamExecutionEnvironment env) throws IOException {
        // Create an instance of the KafkaSource
        final KafkaSource<Ecommerce> source = this.getKafkaSource();

        // Create an instance of the DeltaSink
        final DeltaSink<RowData> sink = this.getDeltaSink(Ecommerce.ECOMMERCE_ROW_TYPE);

        final DataStreamSource<Ecommerce> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        return stream
                .map((MapFunction<Ecommerce, RowData>) Ecommerce::convertToRowData)
                .setParallelism(NUM_SOURCES)
                .sinkTo(sink);
    }

    public void run() throws Exception {
        // Get the Execution Environment (this is like the SparkContext)
        final StreamExecutionEnvironment env = this.getExecutionEnvironment();
        final DataStreamSink<RowData> sink = createDataStream(env);
        sink
                .name("delta-sink")
                .setParallelism(NUM_SINKS)
                .setDescription("writes to Delta Lake");

        // we can now add more to the data stream
        env.execute("kafka-to-delta-sink-job");
    }

}
