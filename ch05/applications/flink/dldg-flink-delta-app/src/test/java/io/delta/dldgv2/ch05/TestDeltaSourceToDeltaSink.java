package io.delta.dldgv2.ch05;

import io.delta.flink.internal.options.DeltaOptionValidationException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static io.delta.dldgv2.ch05.DeltaTestUtils.buildCluster;

public class TestDeltaSourceToDeltaSink {

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(10);

    private String deltaTablePath;

    private static StreamExecutionEnvironment getTestStreamEnv() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }


    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
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
    public void testReadProjectWriteDeltaToDelta() throws DeltaOptionValidationException, Exception {
        final DeltaSourceToDeltaSink app = new DeltaSourceToDeltaSink(new String[] {
                "delta-source-app-w-columns-timestamp.properties"
        });
        var env = app.getExecutionEnvironment();
        var checkpointTempFolder = TEMPORARY_FOLDER.newFolder("checkpoints");
        env.getCheckpointConfig().setCheckpointStorage(app.defaultFs + checkpointTempFolder.getAbsolutePath());

        // ensure we are writing to the temporary location (for tests)
        var checkpointConf = env.getCheckpointConfig();

        // generate the pipeline entities
        var deltaSource = app.getBoundedDeltaSource();
        var sourceRowOutputType = app.getRowTypeFromTypeInformation(deltaSource.getProducedType());
        var deltaSink = app.getDeltaSink(sourceRowOutputType);

        // build the pipeline
        env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(),
                "Delta Lake Source: " + deltaSource.getTablePath().toString())
                .setParallelism(app.NUM_SOURCES)
                .setDescription("DeltaLake source reader")
                .forward()
                .sinkTo(deltaSink)
                .setParallelism(app.NUM_SINKS)
                .setDescription("Writes to DeltaLake using DeltaSink")
                .name("Delta to Delta DataStream");

        final JobExecutionResult result = env.execute("DeltaToDeltaStreamingApp");

    }

}
