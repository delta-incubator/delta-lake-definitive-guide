package io.delta.dldgv2.ch05.pojo;

import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JacksonAnnotation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class Ecommerce {
    @JsonProperty("event_time")
    public String eventTime;
    @JsonProperty("event_type")
    public String eventType;
    @JsonProperty("product_id")
    public int productId;
    @JsonProperty("category_id")
    public long categoryId;
    @JsonProperty("category_code")
    public String categoryCode;
    @JsonProperty("brand")
    public String brand;
    @JsonProperty("price")
    public float price;
    @JsonProperty("user_id")
    public int userId;
    @JsonProperty("user_session")
    public String userSession;


    public Ecommerce() { }

    public Ecommerce(
            final String eventTime,
            final String eventType,
            final int productId,
            final long categoryId,
            final String categoryCode,
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

    public String toString() {
      return "(" + this.eventTime + ", " + this.eventType + ", " + this.productId + ", " + this.categoryId + ", " +
              this.categoryCode + ", " + this.brand + ", " + this.price + ", " + this.userId + ", "
              + this.userSession + ")";
    }

    public static JsonDeserializationSchema<Ecommerce> jsonFormat = new JsonDeserializationSchema<>(Ecommerce.class);

    public static RowData convertToRowData(Ecommerce data) {
        return Ecommerce.ECOMMERCE_ROW_CONVERTER.toInternal(
                Row.of(data.eventTime, data.eventType, data.productId, data.categoryId, data.categoryCode, data.brand,
                        data.price, data.userId, data.userSession)
        );
    }

    public static final RowType ECOMMERCE_ROW_TYPE = new RowType(
          Arrays.asList(
                  new RowType.RowField("event_time", new VarCharType(VarCharType.MAX_LENGTH)),
                  new RowType.RowField("event_type", new VarCharType(VarCharType.MAX_LENGTH)),
                  new RowType.RowField("product_id", new IntType()),
                  new RowType.RowField("category_id", new BigIntType()),
                  new RowType.RowField("category_code", new VarCharType(VarCharType.MAX_LENGTH)),
                  new RowType.RowField("brand", new VarCharType(VarCharType.MAX_LENGTH)),
                  new RowType.RowField("price", new FloatType()),
                  new RowType.RowField("user_id", new IntType()),
                  new RowType.RowField("user_session", new VarCharType(VarCharType.MAX_LENGTH))
          ));

    @SuppressWarnings("unchecked")
    public static final DataFormatConverters.DataFormatConverter<RowData, Row> ECOMMERCE_ROW_CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(ECOMMERCE_ROW_TYPE)
            );


}
