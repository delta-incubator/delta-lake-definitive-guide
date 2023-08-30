package io.delta.dldgv2.ch05.transform;

import io.delta.dldgv2.ch05.format.RowTypes;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class StringToEcomm implements FlatMapFunction<String, RowTypes.EcommerceRow> {

  @Override
  public void flatMap(final String value, final Collector<RowTypes.EcommerceRow> out) {
    for (String token : value.split("\n")) {
      var data = token.split(";");

      try {
        var row = RowTypes.EcommerceRow.stringToRow(token);
        out.collect(row);
      } catch (RuntimeException ex) {
        ex.printStackTrace();
      }
    }
  }

}
