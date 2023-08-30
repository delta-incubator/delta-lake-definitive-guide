package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.format.RowTypes;
import io.delta.dldgv2.ch05.transform.StringToEcomm;
import io.delta.dldgv2.ch05.utils.DeltaUtils;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class FlinkToDeltaSink {

    final public Properties appProps;

    final String propertiesFileName;

    final String defaultTableName = "iot-data";

    public FlinkToDeltaSink(final String[] args) throws IOException {
        this.propertiesFileName = (args.length > 0) ? args[0] : "app.properties";
        this.appProps = DeltaUtils.loadProperties(this.propertiesFileName).orElseThrow();
    }

    public static Logger logger = LoggerFactory.getLogger(FlinkToDeltaSink.class);

    public static void main(final String[] args) throws Exception {
        var deltaSink = new FlinkToDeltaSink(args);
        var deltaTableName = deltaSink.appProps.getProperty("delta.sink.tableName",
            deltaSink.defaultTableName);
        // todo: need to add a tablePath + tableName
        deltaSink.run(deltaTableName);
    }


    public void run(final String tablePath) throws Exception {
        logger.info(String.format("deltaTablePath=%s", tablePath));
        DeltaUtils.prepareDirs(tablePath);
        var brokers = this.appProps.getProperty("kafka.brokers", "127.0.0.1:29092");
        var groupId = this.appProps.getProperty("kafka.group.id", "iot-delta-dldg");
        var topic = this.appProps.getProperty("kafka.topic", "iot-delta");
        final KafkaSource<String> source = this.getFlinkSource(brokers, groupId, topic);
        var deltaTable = this.appProps.getProperty("delta.sink.tableName", "iot-data");

        var sink = this.getDeltaSink(deltaTable, RowTypes.ECOMMERCE_BEHAVIOR);

        var env = this.getExecutionEnvironment();

        env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .setParallelism(1)
            .flatMap(new StringToEcomm())
            .sinkTo(sink)

    }


    /**
     * Sets up the Flink application to read from a Kafka topic
     * @param brokers The address of one or more of the kafka brokers: 127.0.0.1:29092
     * @param groupId Will ensure the stream will pick back up where it left off
     * @param kafkaTopic The name of the Kafka topic
     * @return The KafkaSource instance
     */
    public KafkaSource<String> getFlinkSource(final String brokers, final String groupId, final String kafkaTopic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(kafkaTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public DeltaSink<RowData> getDeltaSink(final String deltaTablePath, final RowType rowType) {
        return DeltaSink
                .forRowData(
                        new Path(deltaTablePath),
                        new Configuration(),
                        rowType
                )
                // withPartitionColumns(partitionCols)
                .build();
    }

    /*public StreamExecutionEnvironment createPipeline(
            Source<T, > source,
            int sourceParallelism,
            int sinkParallelism) {


        StreamExecutionEnvironment env = getExecutionEnvironment();

        // Using Flink Delta Sink in processing pipeline
        env
                .fromSource()

                .setParallelism(sourceParallelism)
                .sinkTo(deltaSink)
                .name("MyDeltaSink")
                .setParallelism(sinkParallelism);

        return env;
    }*/

    public StreamExecutionEnvironment getExecutionEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

}
