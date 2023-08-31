package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.dldgv2.ch05.utils.DeltaUtils;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.file.Paths;

public class FlinkToDeltaSink {
    final static String defaultTableName = "ecomm_v1_clickstream";
    final static String defaultDeltaRootDir = "/tmp/delta";
    final static String defaultCheckpointDir = "/tmp/checkpoints/";
    public static Logger logger = LoggerFactory.getLogger(FlinkToDeltaSink.class);

    final static int NUM_SOURCES = 1;
    final static int NUM_SINKS = 1;

    final String defaultFs;

    final public ParameterTool appProps;

    public FlinkToDeltaSink(final String[] args) throws IOException {
        final String propertiesFileName = (args.length > 0) ? args[0] : "app.properties";
        ClassLoader classLoader = getClass().getClassLoader();
        this.appProps = ParameterTool.fromPropertiesFile(classLoader.getResourceAsStream(propertiesFileName));
        this.defaultFs = this.appProps.get("flink.default.fs.prefix", "file://");
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

    /**
     * Generates a DeltaSink instance leaning on the application properties for config driven development
     * @return The DeltaSink instance bound to generic row data (like Spark DataFrame)
     * @throws IOException when the Delta Table path is non-resolvable
     */
    public DeltaSink<RowData> getDeltaSink() throws IOException {
        // setup all the Delta Lake sink related configurations
        var deltaTable = this.appProps.get("delta.sink.tableName", defaultTableName);
        var deltaTablePath = this.appProps.get("delta.sink.tablePath", defaultDeltaRootDir);
        var fullDeltaTablePath = Paths.get(deltaTablePath, deltaTable).toString();
        var deleteTableIfExists = this.appProps
                .getBoolean("runtime.cleaner.delete.deltaTable", false);

        DeltaUtils.prepareDirs(fullDeltaTablePath, deleteTableIfExists);
        logger.info(String.format("deltaTablePath=%s  deleteIfExists?=%b", fullDeltaTablePath, deleteTableIfExists));
        boolean mergeSchema = this.appProps.getBoolean("delta.sink.mergeSchema", false);
        return DeltaSink
                .forRowData(
                        new Path(fullDeltaTablePath),
                        new Configuration(),
                        Ecommerce.ECOMMERCE_ROW_TYPE
                )
                .withMergeSchema(mergeSchema)
                .build();
    }

    /**
     * Generates the runtime Execution Environment for Apache Flink
     * @return The StreamExecutionEnvironment handle for the application
     */
    public StreamExecutionEnvironment getExecutionEnvironment() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        if (this.appProps.getBoolean("flink.state.backend.enabled", false)) {
            env.setStateBackend(new HashMapStateBackend());
            var checkpointDir = this.appProps.get("flink.checkpoint.storage.dir", defaultCheckpointDir);
            env.getCheckpointConfig().setCheckpointStorage(this.defaultFs + "/" + checkpointDir);
        }

        return env;
    }

}
