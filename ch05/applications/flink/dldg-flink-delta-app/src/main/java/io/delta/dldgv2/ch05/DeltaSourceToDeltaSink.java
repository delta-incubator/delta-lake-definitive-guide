package io.delta.dldgv2.ch05;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

public class DeltaSourceToDeltaSink extends DeltaFlinkBase {
    public final int NUM_SOURCES = 1;
    public final int NUM_SINKS = 1;

    public DeltaSourceToDeltaSink(final String[] args) throws IOException {
        super(args);
    }

    public static void main(final String[] args) throws Exception {
        new DeltaSourceToDeltaSink(args).run();
    }

    public void run() throws Exception {

        // create the streaming environment (for the Data Stream API)
        final StreamExecutionEnvironment env = this.getExecutionEnvironment();

        // Build the DeltaSource (using the isBounded flag to switch between RuntimeExecutionMode (BATCH, STREAMING))
        // the StreamExecutionEnvironment is set to AUTOMATIC, but you can also specify BATCH or STREAMING
        // to honor the intention of the Flink application
        final DeltaSource<RowData> source =
                (this.appProps.getBoolean("delta.source.isBounded", true)) ?
                        this.getBoundedDeltaSource() : this.getContinuousDeltaSource();

        // note: transformations would help to further refine the resulting
        // type being written into the DeltaSink
        // you can use the


        final RowType rowTypeForSink = getRowTypeFromTypeInformation(source.getProducedType());
        final DeltaSink<RowData> sink = getDeltaSink(rowTypeForSink);

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(),
                        "Delta Lake Source: " + source.getTablePath().toString())
                .setParallelism(NUM_SOURCES)
                .setDescription("DeltaLake source reader")
                .sinkTo(sink)
                .setParallelism(NUM_SINKS)
                .setDescription("Writes to DeltaLake using DeltaSink.")
                .name("Delta to Delta DataStream");

        final JobExecutionResult result = env.execute("DeltaToDeltaStreamingApp");
        logger.info(String.format("job.completed result=%s", result.toString()));
    }

}
