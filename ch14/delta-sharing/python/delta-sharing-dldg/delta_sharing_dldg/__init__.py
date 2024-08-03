from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta_sharing_dldg.sharing import Sharing


def main(sharing_client: Sharing, spark_session: SparkSession):
    shares = sharing_client.list_shares()
    schemas = sharing_client.list_schemas(shares[0])
    tables = sharing_client.list_tables(schemas[0])

    lending_tree = tables[3]
    table_url = sharing_client.table_url(lending_tree)

    df = (
        spark_session
        .read
        .format("deltaSharing")
        .load(table_url)
    )

    df.printSchema()

    (df
     .select(
        col("loan_amnt"),
        col("funded_amnt"),
        col("term"),
        col("grade"),
        col("home_ownership"),
        col("annual_inc"),
        col("loan_status")
     )
     .show()
     )

if __name__ == "__main__":
    client = Sharing(sharing_profile="profiles/open-datasets.share")
    spark: SparkSession = (
        SparkSession.builder
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .appName("delta_sharing_dldg")
        .getOrCreate()
    )

    main(client, spark)
