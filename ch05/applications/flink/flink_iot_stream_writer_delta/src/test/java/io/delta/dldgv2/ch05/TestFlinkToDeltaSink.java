package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.flink.sink.DeltaSink;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.types.StructField;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.delta.dldgv2.ch05.DeltaTestUtils.buildCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFlinkToDeltaSink {

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(10);

    private String deltaTablePath;

    //private Path savepointPath;


    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    private static StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    @BeforeEach
    public void setup() throws IOException {
        try {
            miniClusterResource.before();
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
        deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        //savepointPath = TEMPORARY_FOLDER.newFolder().toPath();
    }

    @AfterEach
    public void teardown() {
        miniClusterResource.after();
    }

    @Test
    public void testWriteToDeltaTable() throws Exception {

        final StreamExecutionEnvironment env = getTestStreamEnv();
        env.registerType(Ecommerce.class);
        env.getConfig().enableForceAvro();

        final GeneratorFunction<Long, RowData> generatorFunction = DeltaTestUtils.DataGenUtil::generateNextRow;

        long numberOfRecords = 1000;
        double recordsPerSecond = 500;
        int numSources = 1;
        int numSinks = 1;

        final InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(Ecommerce.ECOMMERCE_ROW_TYPE);

        final DataGeneratorSource<RowData> source =
                new DataGeneratorSource<>(
                        generatorFunction,
                        numberOfRecords,
                        RateLimiterStrategy.perSecond(recordsPerSecond),
                        typeInfo);

        // create the DataStreamSource and begin the Job Graph
        final DataStreamSource<RowData> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Ecommerce Generator Source");
        // we only have one source
        stream.setParallelism(numSources);

        // delta info
        org.apache.flink.core.fs.Path deltaTable = new org.apache.flink.core.fs.Path(deltaTablePath);
        var config = new Configuration();
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        deltaTable,
                        config,
                        Ecommerce.ECOMMERCE_ROW_TYPE
                )

                .withMergeSchema(false)
                // withPartitionColumns(partitionCols)
                .build();

        stream.sinkTo(deltaSink);
        stream.name("DeltaSink");
        stream.setParallelism(numSinks);

        // run the flink job
        // this will block and that is good since we can test that things all worked as expected
        env.execute();


        //final DeltaSource<RowData> deltaSourceData = DeltaSource.forBoundedRowData(deltaTable, config).build();

        // now that the application has run through all the records, and created the Delta Table.
        // we can read it back

        final DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);

        var records = DeltaTestUtils.ParquetUtil.readAndValidateAllTableRecords(deltaLog);

        assertEquals(records, numberOfRecords);

        // this is the first time creating the table, and it will be removed at the end of the test
        long initialVersion = deltaLog.snapshot().getVersion();
        assertEquals(initialVersion, 0L);

        var tableMetadata = deltaLog.snapshot().getMetadata();

        var schema = tableMetadata.getSchema();

        var fields = Arrays.stream(schema.getFields())
                .map(StructField::getName)
                .collect(Collectors.toSet());
        assertEquals(fields.size(), Ecommerce.ECOMMERCE_ROW_TYPE.getFieldNames().size());

        assertTrue(deltaLog.tableExists());
    }

    @Test
    public void TestRecordGenerator() throws Exception {

        var rows = Stream
                .generate(new DeltaTestUtils.DataGenUtil.RowSupplier())
                .limit(1000)
                .collect(Collectors.toList());

        assertEquals(1000, rows.size());
    }

    /**
     * Runs Flink job in a daemon thread.
     * <p>
     * This workaround is needed because if we try to first run the Flink job and then query the
     * table with Delta Standalone Reader (DSR) then we are hitting "closed classloader exception"
     * which in short means that finished Flink job closes the classloader for the classes that DSR
     * tries to reuse.
     *
     * @param rowType  structure of the events in the streaming job
     * @param testData collection of test {@link RowData}
     */
    private void runFlinkJobInBackground(RowType rowType,
                                         List<RowData> testData) {
        new Thread(() -> runFlinkJob(rowType, testData)).start();
    }

    private void runFlinkJob(RowType rowType,
                             List<RowData> testData) {
        StreamExecutionEnvironment env = getTestStreamEnv();
        env.registerType(Ecommerce.class);

        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new org.apache.flink.core.fs.Path(deltaTablePath),
                        DeltaTestUtils.getHadoopConf(), rowType)
                .build();
        env.fromCollection(testData).sinkTo(deltaSink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void runFlinkJobInBackground(StreamExecutionEnvironment env) {
        new Thread(() -> {
            try (MiniCluster miniCluster = getMiniCluster()) {
                miniCluster.start();
                miniCluster.executeJobBlocking(env.getStreamGraph().getJobGraph());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();
    }

    public MiniCluster getMiniCluster() {
        final org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(2)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        return new MiniCluster(cfg);
    }

}
