package io.delta.dldgv2.ch05;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.delta.dldgv2.ch05.DeltaTestUtils.buildCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDeltaSourceToDeltaSink {

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(10);

    private String deltaTablePath;

    private static StreamExecutionEnvironment getTestStreamEnv() throws IOException {
        return new DeltaSourceToDeltaSink(new String[]{ "" }).getExecutionEnvironment();
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
    public void testReadProjectWriteDeltaToDelta() throws Exception {
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

    /*
    var streamGraph = env.getStreamGraph();
        final StringJoiner joiner = new StringJoiner(">");
        streamGraph.getStreamNodes().stream().map(StreamNode::getOperatorDescription).forEach(joiner::add);
        var theGraph = joiner.toString();
     */


}
