package io.delta.dldgv2.ch05;

import io.delta.dldgv2.ch05.pojo.Ecommerce;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class DeltaTestUtils {

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

    public static void prepareDirs(String tablePath) throws IOException {
        File tableDir = new File(tablePath);
        if (tableDir.exists()) {
            FileUtils.cleanDirectory(tableDir);
        } else {
            tableDir.mkdirs();
        }
    }

    public static void prepareDirs(String sourcePath, String workPath) throws IOException {
        prepareDirs(workPath);
        System.out.printf("Copy example table data from %s to %s%n%n", sourcePath, workPath);
        FileUtils.copyDirectory(new File(sourcePath), new File(workPath));
    }

    public static MiniClusterWithClientResource buildCluster(int slotPerTaskManager) {
        Configuration configuration = new Configuration();

        // By default, let's check for leaked classes in tests.
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, true);

        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberTaskManagers(1)
                        .setNumberSlotsPerTaskManager(slotPerTaskManager)
                        .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                        .withHaLeadershipControl()
                        .setConfiguration(configuration)
                        .build());
    }

    public static class ParquetUtil {
        public static int readAndValidateAllTableRecords(DeltaLog deltaLog) throws IOException {
            final List<AddFile> deltaTableFiles = deltaLog.snapshot().getAllFiles();
            int cumulatedRecords = 0;
            for (AddFile addedFile : deltaTableFiles) {
                Path parquetFilePath = new Path(deltaLog.getPath().toString(), addedFile.getPath());
                cumulatedRecords += ParquetUtil.parseAndCountRecords(
                        parquetFilePath,
                        Ecommerce.ECOMMERCE_ROW_TYPE,
                        Ecommerce.ECOMMERCE_ROW_CONVERTER
                );
            }
            return cumulatedRecords;
        }

        public static int parseAndCountRecords(
                Path parquetFilepath,
                RowType rowType,
                DataFormatConverters.DataFormatConverter<RowData, Row> converter) throws IOException {
            ParquetColumnarRowSplitReader reader = getTestParquetReader(
                    parquetFilepath,
                    rowType
            );

            int recordsRead = 0;
            while (!reader.reachedEnd()) {
                converter.toExternal(reader.nextRecord());
                recordsRead++;
            }
            return recordsRead;
        }

        private static ParquetColumnarRowSplitReader getTestParquetReader(
                Path path, RowType rowType) throws IOException {
            return ParquetSplitReaderUtil.genPartColumnarRowReader(
                    true, // utcTimestamp
                    true, // caseSensitive
                    DeltaTestUtils.getHadoopConf(),
                    rowType.getFieldNames().toArray(new String[0]),
                    rowType.getChildren().stream()
                            .map(TypeConversions::fromLogicalToDataType)
                            .toArray(DataType[]::new),
                    new HashMap<>(),
                    IntStream.range(0, rowType.getFieldCount()).toArray(),
                    50,
                    path,
                    0,
                    Long.MAX_VALUE);
        }
    }

    public static class DataGenUtil {
        private static final DecimalFormat df = new DecimalFormat("0.00");
        public static Instant startingTime = Instant.parse("2023-08-30T00:00:00.00Z");
        public static Random random = new Random(123L);


        // Stores the list of categories
        public static List<Long> categories = Arrays.asList(
                2053013552326770905L, 2053013559792632471L, 2053013565480109009L,
                2053013555631882655L, 2053013557099889147L, 2053013554415534427L);

        // Stores the list of category codes
        public static List<String> categoryCodes = Arrays.asList(
                "appliances.environment.water_heater", "furniture.living_room.sofa", "apparel.shoes",
                "electronics.smartphone", "furniture.bedroom.bed", "electronics.video.tv"
        );

        // Stores the lookup table for our psuedo-random ecommerce data generator
        public static Map<String, List<String>> brands = new HashMap<>();

        // priceBounds contains the upper and lower bounds for creating human beliveable prices per category
        public static Map<String, List<Integer>> priceBounds = new HashMap<>();

        static {
            brands.put("appliances.environment.water_heater",
                    Arrays.asList("heater1", "heater2", "heater3", "heater4"));
            priceBounds.put("appliances.environment.water_heater", Arrays.asList(1000, 3000));
            brands.put("furniture.living_room.sofa",
                    Arrays.asList("restoration_hardware", "pottery_barn", "ikea"));
            priceBounds.put("furniture.living_room.sofa", Arrays.asList(1000, 20000));
            brands.put("apparel.shoes", Arrays.asList("nike", "keds", "new_balance", "stride_right"));
            priceBounds.put("apparel.shoes", Arrays.asList(100, 500));
            brands.put("electronics.smartphone", Arrays.asList("samsung", "apple", "google"));
            priceBounds.put("electronics.smartphone", Arrays.asList(200, 1400));
            brands.put("furniture.bedroom.bed", Arrays.asList("beauty_rest", "sealy", "sterns_and_foster"));
            priceBounds.put("furniture.bedroom.bed", Arrays.asList(1000, 8000));
            brands.put("electronics.video.tv", Arrays.asList("samsung", "lg", "sony"));
            priceBounds.put("electronics.video.tv", Arrays.asList(2000, 8000));
        }

        public static int clampedInt(Random rand, int min, int max) {
            int next = rand.nextInt(max);
            return (next < min) ? min : next;
        }

        public static RowData generateNextRow(long generatorOffset) {
            final Instant nextRecordTime = startingTime.plusSeconds(generatorOffset * 30);
            final int productId = random.nextInt(50000);
            int categoryIndex = random.nextInt(categories.size() - 1);
            final long categoryId = categories.get(categoryIndex);
            final String categoryCode = categoryCodes.get(categoryIndex);
            var possibleBrands = brands.get(categoryCode);
            final String brand = possibleBrands.get(random.nextInt(brands.get(categoryCode).size() - 1));
            var bounds = priceBounds.get(categoryCode);
            final UUID userSession = UUID.randomUUID();
            Ecommerce pojo = new Ecommerce(
                    nextRecordTime.toString(),
                    "view",
                    productId,
                    categoryId,
                    categoryCode,
                    brand,
                    Float.parseFloat(df.format(clampedInt(random, bounds.get(0), bounds.get(1)))),
                    random.nextInt(200),
                    userSession.toString()
            );
            return Ecommerce.convertToRowData(pojo);
        }

        public static class RowSupplier implements Supplier<RowData> {
            private final AtomicLong value = new AtomicLong(1L);

            @Override
            public RowData get() {
                return generateNextRow(value.getAndIncrement());
            }
        }
    }

}
