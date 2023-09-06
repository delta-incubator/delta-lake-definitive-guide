package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.utils.DeltaUtils;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.RowDataDeltaSinkBuilder;
import io.delta.flink.source.DeltaSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
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

public abstract class DeltaFlinkBase {

    final public ParameterTool appProps;
    // if running in continuous mode - set how often to check if new records have arrived
    final long checkSourceForNewDataInterval = 2000L;
    final String defaultFs;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
    int NUM_SOURCES = 1;
    int NUM_SINKS = 1;
    String defaultSourceTableName = "ecomm_v1_clickstream";
    String defaultSinkTableName = "ecomm_v1_clickstream_lite";
    String defaultDeltaRootDir = "/tmp/delta";
    String defaultCheckpointDir = "/tmp/checkpoints/";

    public DeltaFlinkBase(final String[] args) throws IOException {
        final String propertiesFileName = (args.length > 0) ? args[0] : "delta-source-app.properties";
        ClassLoader classLoader = getClass().getClassLoader();
        this.appProps = ParameterTool.fromPropertiesFile(classLoader.getResourceAsStream(propertiesFileName));
        this.defaultFs = this.appProps.get("flink.default.fs.prefix", "file://");
    }

    /**
     * Generates the runtime Execution Environment for Apache Flink
     *
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

    /**
     * combines the configuration and returns the flink.fs.path to the Delta Lake location
     *
     * @return The Delta Lake table path for the Sink
     */
    public Path getDeltaTableForSink() {
        var deltaTablePath = this.appProps.get("delta.sink.tablePath", defaultDeltaRootDir);
        var deltaTable = this.appProps.get("delta.sink.tableName", defaultSinkTableName);
        return new Path(Paths.get(deltaTablePath, deltaTable).toString());
    }

    /**
     * Generates a DeltaSink instance leaning on the application properties for config driven development
     *
     * @return The DeltaSink instance bound to generic row data (like Spark DataFrame)
     * @throws IOException when the Delta Table path is non-resolvable
     */
    public DeltaSink<RowData> getDeltaSink(final RowType rowType) throws IOException {
        var deleteTableIfExists = this.appProps
                .getBoolean("runtime.cleaner.delete.deltaTable", false);
        Path deltaTablePath = getDeltaTableForSink();

        DeltaUtils.prepareDirs(deltaTablePath.getPath(), deleteTableIfExists);
        logger.info(String.format("deltaTablePath=%s  deleteIfExists?=%b", deltaTablePath.getPath(), deleteTableIfExists));
        boolean mergeSchema = this.appProps.getBoolean("delta.sink.mergeSchema", false);

        final RowDataDeltaSinkBuilder deltaSinkBuilder = DeltaSink.forRowData(
                deltaTablePath,
                new Configuration(),
                rowType
        );
        // todo - follow up with example using delta sink options (the documentation eludes me)
        getPartitionColumnsForSink().map(list -> deltaSinkBuilder.withPartitionColumns(list.toArray(new String[0])));
        deltaSinkBuilder.withMergeSchema(mergeSchema);

        // todo - set options based on configs (options take string, boolean, int and long values)

        return deltaSinkBuilder.build();
    }

    /**
     * Simplify fetching the partition columns for the Delta Sink
     *
     * @return The Optional List of String column names
     */
    public Optional<List<String>> getPartitionColumnsForSink() {
        final String confName = "delta.sink.partitionCols";
        final List<String> partitionCols = Arrays.stream(
                this.appProps.get(confName, StringUtils.EMPTY)
                        .split(",")).map(String::trim).collect(Collectors.toList());
        return (partitionCols.isEmpty()) ? Optional.empty() : Optional.of(partitionCols);
    }

    /**
     * Generate a Bounded DeltaSource. This can be used for Batch mode
     * Use the config values in the application properties to control table projection,
     * as well as starting position (version or timestamp) :
     * for projection: delta.source.columnNames.enabled & delta.source.columnNames
     * for starting from a version: delta.source.versionAsOf.enabled & delta.source.versionAsOf
     * for starting from the closest timestamp: delta.source.timestampAsOf.enabled & delta.source.timestampAsOf
     *
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
     *
     * link <a href="https://github.com/delta-io/connectors/blob/master/flink/README.md#continuous-mode">config</a>
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
     *
     * @return The Delta Lake table path for the Source
     */
    public Path getDeltaTableForSource() {
        var deltaTablePath = this.appProps.get("delta.source.tablePath", defaultDeltaRootDir);
        var deltaTable = this.appProps.get("delta.source.tableName", defaultSourceTableName);
        return new Path(Paths.get(deltaTablePath, deltaTable).toString());
    }

    /**
     * This method does some casting to expose internal methods on TypeInformation
     *
     * @param typeInfo The TypeInformation Object
     * @return The RowType as declared through the fields of the initial TypeInformation
     */
    public RowType getRowTypeFromTypeInformation(final TypeInformation<RowData> typeInfo) {
        final InternalTypeInfo<RowData> sourceType = (InternalTypeInfo<RowData>) typeInfo;
        return (RowType) sourceType.toLogicalType();
    }

    /**
     * Tests if column name projection is enabled, then returns an Optional of the column names or empty
     *
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
     *
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
     *
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

}
