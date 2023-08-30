package io.delta.dldgv2.ch05.format;

import org.apache.flink.table.types.logical.*;

import java.util.Arrays;

public final class RowTypes {

  private RowTypes() {}

  public static final RowType ECOMMERCE_BEHAVIOR = new RowType(
      Arrays.asList(
          new RowType.RowField("event_time", new VarCharType()),
          new RowType.RowField("event_type", new VarCharType()),
          new RowType.RowField("product_id", new IntType()),
          new RowType.RowField("category_id", new BigIntType()),
          new RowType.RowField("category_code", new BigIntType()),
          new RowType.RowField("brand", new VarCharType()),
          new RowType.RowField("price", new FloatType()),
          new RowType.RowField("user_id", new IntType()),
          new RowType.RowField("user_session", new VarCharType())
      )
  );

  public static class EcommerceRow {
    public String eventTime;
    public String eventType;
    public int productId;
    public long categoryId;
    public long categoryCode;
    public String brand;
    public float price;
    public int userId;
    public String userSession;

    public EcommerceRow() {}

    public EcommerceRow(
        final String eventTime,
        final String eventType,
        final int productId,
        final long categoryId,
        final long categoryCode,
        final String brand,
        final float price,
        final int userId,
        final String userSession) {
      this.eventTime = eventTime;
      this.eventType = eventType;
      this.productId = productId;
      this.categoryId = categoryId;
      this.categoryCode = categoryCode;
      this.brand = brand;
      this.price = price;
      this.userId = userId;
      this.userSession = userSession;
    }

    public static EcommerceRow stringToRow(final String token) throws RuntimeException {
      var parts = token.split(";");
      if (parts.length < 8) throw new RuntimeException("bad data");

      return new EcommerceRow();
    }

  }

}
