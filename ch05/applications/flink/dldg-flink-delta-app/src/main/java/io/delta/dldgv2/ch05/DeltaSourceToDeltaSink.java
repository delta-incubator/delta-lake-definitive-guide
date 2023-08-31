package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.RowDataBoundedDeltaSourceBuilder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DeltaSourceToDeltaSink {
    public static Logger logger = LoggerFactory.getLogger(DeltaSourceToDeltaSink.class);
    final static int NUM_SOURCES = 1;
    final static int NUM_SINKS = 1;
    final static String defaultSourceTableName = "ecomm_v1_clickstream";
    final static String defaultSinkTableName = "ecomm_v1_clickstream_lite";
    final static String defaultDeltaRootDir = "/tmp/delta";
    final static String defaultCheckpointDir = "/tmp/checkpoints/";

    // if running in continuous mode - set how often to check if new records have arrived
    final long checkSourceForNewDataInterval = 2000L;

    public static void main(final String[] args) throws Exception {
        new DeltaSourceToDeltaSink(args).run();
    }

    final String defaultFs;

    final public ParameterTool appProps;

    public DeltaSourceToDeltaSink(final String[] args) throws IOException {
        final String propertiesFileName = (args.length > 0) ? args[0] : "delta-source-app.properties";
        ClassLoader classLoader = getClass().getClassLoader();
        this.appProps = ParameterTool.fromPropertiesFile(classLoader.getResourceAsStream(propertiesFileName));
        this.defaultFs = this.appProps.get("flink.default.fs.prefix", "file://");
    }

    public void run() throws Exception {
        // get the source: supports batch (bounded) and streaming (continuous) runtime modes.
        final DeltaSource<RowData> source =
                (this.appProps.getBoolean("delta.source.isBounded", true)) ?
                this.getBoundedDeltaSource() : this.getContinuousDeltaSource();

        // todo - add the DeltaSink<RowData> - automatically generating the RowType using the source
        /*
        - //source.getProducedType()
        - via
        rowType.getFieldNames().toArray(new String[0]),
                    rowType.getChildren().stream()
                            .map(TypeConversions::fromLogicalToDataType)
                            .toArray(DataType[]::new),
         */

        final StreamExecutionEnvironment env = this.getExecutionEnvironment();

        // todo - need to extract the "projected" source
        // into a known type. This needs to be done on the fly
        // and needs to support the `columns` projection too...
        /*var sinkTypeInfo = new RowTypeInfo(source.getProducedType());
        var serializer = sinkTypeInfo.createSerializer(new ExecutionConfig());
        var row = serializer.createInstance();*/

        // todo - this needs to support config based projection (could use the configuration)
        // the internal type info can convert to RowType - but this is odd. Until tomorrow here.
        // ((RowType) ((InternalTypeInfo) ((RowDataFormat) deltaBoundedSource.readerFormat).decoratedInputFormat.producedTypeInfo).type)
        final DeltaSink<RowData> sink = DeltaSink
                .forRowData(
                        getDeltaTableForSink(),
                        new Configuration(),
                        Ecommerce.ECOMMERCE_ROW_TYPE
                ).build();

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(),
                        "Delta Lake Source: " + source.getTablePath().toString())
                .setParallelism(NUM_SOURCES)
                .setDescription("DeltaLake source reader")
                .sinkTo(sink)
                .setParallelism(NUM_SINKS)
                .setDescription("Writes to DeltaLake using DeltaSink.")
                .name("Delta to Delta DataStream");

        env.execute();

    }

    /**
     * Generate a Bounded DeltaSource. This can be used for Batch mode
     * Use the config values in the application properties to control table projection,
     * as well as starting position (version or timestamp) :
     * for projection: delta.source.columnNames.enabled & delta.source.columnNames
     * for starting from a version: delta.source.versionAsOf.enabled & delta.source.versionAsOf
     * for starting from the closest timestamp: delta.source.timestampAsOf.enabled & delta.source.timestampAsOf
     * @return The DeltaSource
     */
    public DeltaSource<RowData> getBoundedDeltaSource() {
        var sourceDeltaTable = getDeltaTableForSource();
        logger.info(String.format("flink.source.delta.table.path=%s", sourceDeltaTable.toString()));

        // Provide a pluggable builder
        var deltaSourceBuilder = DeltaSource.forBoundedRowData(
                sourceDeltaTable,
                new Configuration()
        );

        getSourceColumnNames().map(deltaSourceBuilder::columnNames);
        getStartingVersion().map(deltaSourceBuilder::versionAsOf);
        getTimestampAsOf().map(deltaSourceBuilder::timestampAsOf);

        return deltaSourceBuilder.build();
    }

    /**
     * Generate a Continuous DeltaSource. This can be used for real-time mode
     * Use the config values in the application properties to control table projection,
     * as well as starting position (version or timestamp) :
     * for projection: delta.source.columnNames.enabled & delta.source.columnNames
     * for starting from a version: delta.source.versionAsOf.enabled & delta.source.versionAsOf
     * for starting from the closest timestamp: delta.source.timestampAsOf.enabled & delta.source.timestampAsOf
     * @return The DeltaSource
     */
    public DeltaSource<RowData> getContinuousDeltaSource() {
        var sourceDeltaTable = getDeltaTableForSource();
        logger.info(String.format("flink.source.delta.table.path=%s", sourceDeltaTable.toString()));

        var deltaSourceBuilder = DeltaSource.forContinuousRowData(
                sourceDeltaTable,
                new Configuration()
        ).updateCheckIntervalMillis(checkSourceForNewDataInterval);

        getSourceColumnNames().map(deltaSourceBuilder::columnNames);
        getStartingVersion().map(deltaSourceBuilder::startingVersion);
        getTimestampAsOf().map(deltaSourceBuilder::startingTimestamp);

        return deltaSourceBuilder.build();
    }

    /**
     * combines the configuration and returns the flink.fs.path to the Delta Lake location
     * @return The Delta Lake table path for the Source
     */
    public Path getDeltaTableForSource() {
        var deltaTablePath = this.appProps.get("delta.source.tablePath", defaultDeltaRootDir);
        var deltaTable = this.appProps.get("delta.source.tableName", defaultSourceTableName);
        return new Path(Paths.get(deltaTablePath, deltaTable).toString());
    }

    /**
     * combines the configuration and returns the flink.fs.path to the Delta Lake location
     * @return The Delta Lake table path for the Sink
     */
    public Path getDeltaTableForSink() {
        var deltaTablePath = this.appProps.get("delta.sink.tablePath", defaultDeltaRootDir);
        var deltaTable = this.appProps.get("delta.sink.tableName", defaultSinkTableName);
        return new Path(Paths.get(deltaTablePath, deltaTable).toString());
    }

    /**
     * Tests if column name projection is enabled, then returns an Optional of the column names or empty
     * @return The Optional
     */
    public Optional<List<String>> getSourceColumnNames() {
        if (this.appProps.getBoolean("delta.source.columnNames.enabled", false)) {
            var cols = Arrays.stream(
                            this.appProps.get("delta.source.columnNames", "").split(","))
                    .map(String::trim).collect(Collectors.toList());
            return (!cols.isEmpty()) ? Optional.of(cols) : Optional.empty();
        } else return Optional.empty();
    }

    /**
     * Tests if the versionAsOf is enabled and returns the long value representing the Delta Log Transaction pointer
     * @return The Optional transaction number to begin reading for the DeltaSource
     */
    public Optional<Long> getStartingVersion() {
        if (this.appProps.getBoolean("delta.source.versionAsOf.enabled", false)) {
            // we expect to fail if the value is non-parsable.
            return Optional.of(this.appProps.getLong("delta.source.versionAsOf"));
        } else return Optional.empty();
    }

    /**
     * Tests if the timestampAsOf is enabled and returns the string based timestamp (iso8601 or supported format)
     * @return The Optional timestamp for the DeltaStandalone library to find (Scan)
     * to represent the initial Snapshot pointer.
     */
    public Optional<String> getTimestampAsOf() {
        if (this.appProps.getBoolean("delta.source.timestampAsOf.enabled", false)) {
            // we expect this to fail if the value is non-parsable. Additionally, the timestamp
            // format must conform to internal flink supplier
            // io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter
            // when in doubt - use ISO8601 format
            return Optional.of(this.appProps.get("delta.source.timestampAsOf"));
        } else return Optional.empty();
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
