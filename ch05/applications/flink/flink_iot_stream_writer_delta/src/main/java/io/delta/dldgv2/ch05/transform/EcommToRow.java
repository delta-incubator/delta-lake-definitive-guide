package io.delta.dldgv2.ch05.transform;

import io.delta.dldgv2.ch05.format.RowTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class EcommToRow {

  public static final DataFormatConverters.DataFormatConverter CONVERTER =
      DataFormatConverters.getConverterForDataType(
          TypeConversions.fromLogicalToDataType(RowTypes.ECOMMERCE_BEHAVIOR)
      );

  public static RowData transform(RowTypes.EcommerceRow row) {

    var transformed = Row.of(row.eventTime, row.eventType, row.productId,
        row.categoryId, row.categoryCode, row.brand, row.price,
        row.userId, row.userSession);

    // todo - pick up from here.

    RowData rd = CONVERTER.toInternal(transformed);

    return rd;
  }

}
