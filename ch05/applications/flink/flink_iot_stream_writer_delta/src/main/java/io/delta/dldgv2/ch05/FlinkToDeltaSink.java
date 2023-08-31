package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.dldgv2.ch05.utils.DeltaUtils;
import io.delta.flink.sink.DeltaSink;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FlinkToDeltaSink {
    final static String defaultTableName = "ecomm_v1_clickstream";
    final static int NUM_SOURCES = 1;
    final static int NUM_SINKS = 1;
    public static Logger logger = LoggerFactory.getLogger(FlinkToDeltaSink.class);
    final public ParameterTool appProps;

    public FlinkToDeltaSink(final String[] args) throws IOException {
        final String propertiesFileName = (args.length > 0) ? args[0] : "app.properties";
        ClassLoader classLoader = getClass().getClassLoader();
        this.appProps = ParameterTool.fromPropertiesFile(classLoader.getResourceAsStream(propertiesFileName));
    }

    public static void main(final String[] args) throws Exception {
        new FlinkToDeltaSink(args).run();
    }

    public void run() throws Exception {

        // Create an instance of the KafkaSource
        final KafkaSource<Ecommerce> source = this.getKafkaSource();

        // Create an instance of the DeltaSink
        final DeltaSink<RowData> sink = this.getDeltaSink();

        // Get the Execution Environment (this is like the SparkContext)
        final StreamExecutionEnvironment env = this.getExecutionEnvironment();

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<Ecommerce, RowData>) Ecommerce::convertToRowData)
                .setParallelism(NUM_SOURCES)
                .sinkTo(sink)
                .name("DeltaSink")
                .setParallelism(NUM_SINKS);

        env.execute();
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

    public DeltaSink<RowData> getDeltaSink() throws IOException {
        // setup all the Delta Lake sink related configurations
        var deltaTable = this.appProps.get("delta.sink.tableName", defaultTableName);
        var deltaTablePath = this.appProps.get("delta.sink.tablePath", "/tmp/delta");
        var fullDeltaTablePath = Paths.get(deltaTablePath, deltaTable).toString();
        var deleteTableIfExists = this.appProps
                .getBoolean("runtime.cleaner.delete.deltaTable", false);

        DeltaUtils.prepareDirs(fullDeltaTablePath, deleteTableIfExists);
        logger.info(String.format("deltaTablePath=%s  deleteIfExists?=%b", fullDeltaTablePath, deleteTableIfExists));
        boolean mergeSchema = this.appProps.getBoolean("delta.sink.mergeSchema", false);
        return DeltaSink
                .forRowData(
                        new Path(deltaTablePath),
                        new Configuration(),
                        Ecommerce.ECOMMERCE_ROW_TYPE
                )
                .withMergeSchema(mergeSchema)
                .build();
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

}
