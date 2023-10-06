package io.delta.dldgv2.ch05.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class DeltaUtils {

    public static void prepareDirs(final String tablePath, boolean deleteIfExists) throws IOException {
        File tableDir = new File(tablePath);
        if (tableDir.exists() && deleteIfExists) {
            FileUtils.cleanDirectory(tableDir);
        } else {
            tableDir.mkdirs();
        }
    }
}
