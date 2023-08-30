package io.delta.dldgv2.ch05;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

public class TestFlinkToDeltaSink {

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String deltaTablePath;


    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setup() {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Test
    public void testWriteToDeltaTable() throws Exception {
        // GIVEN
        RowType rowType = new RowType(
                Arrays.asList(
                        new RowType.RowField("f1", new FloatType()),
                        new RowType.RowField("f2", new IntType()),
                        new RowType.RowField("f3", new VarCharType()),
                        new RowType.RowField("f4", new DoubleType()),
                        new RowType.RowField("f5", new BooleanType()),
                        new RowType.RowField("f6", new TinyIntType()),
                        new RowType.RowField("f7", new SmallIntType()),
                        new RowType.RowField("f8", new BigIntType()),
                        new RowType.RowField("f9", new BinaryType()),
                        new RowType.RowField("f10", new VarBinaryType()),
                        new RowType.RowField("f11", new TimestampType()),
                        new RowType.RowField("f12", new LocalZonedTimestampType()),
                        new RowType.RowField("f13", new DateType()),
                        new RowType.RowField("f14", new CharType()),
                        new RowType.RowField("f15", new DecimalType()),
                        new RowType.RowField("f16", new DecimalType(4, 2))
                ));

        Integer value = 1;

        Row testRow = Row.of(
                value.floatValue(), // float type
                value, // int type
                value.toString(), // varchar type
                value.doubleValue(), // double type
                false, // boolean type
                value.byteValue(), // tiny int type
                value.shortValue(), // small int type
                value.longValue(), // big int type
                String.valueOf(value).getBytes(StandardCharsets.UTF_8), // binary type
                String.valueOf(value).getBytes(StandardCharsets.UTF_8), // varbinary type
                LocalDateTime.now(ZoneOffset.systemDefault()), // timestamp type
                Instant.now(), // local zoned timestamp type
                LocalDate.now(), // date type
                String.valueOf(value), // char type
                BigDecimal.valueOf(value), // decimal type
                new BigDecimal("11.11") // decimal(4,2) type
        );


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
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path(deltaTablePath),
                        DeltaTestUtils.getHadoopConf(), rowType).build();
        env.fromCollection(testData).sinkTo(deltaSink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

}
