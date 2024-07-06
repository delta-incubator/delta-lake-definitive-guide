import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta_sharing_dldg.sharing import Sharing
from delta_sharing import Share, Schema, Table


class TestSharing(unittest.TestCase):

    @staticmethod
    def spark_session() -> SparkSession:
        spark: SparkSession = (
            SparkSession.builder
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .appName("delta_sharing_dldg")
            .getOrCreate()
        )
        return spark

    def test_all_methods(self):
        sharing_client: Sharing = Sharing(sharing_profile="profiles/open-datasets.share")

        shares = sharing_client.list_shares()
        first_share: Share = shares[0]

        schemas = sharing_client.list_schemas(first_share)
        first_schema: Schema = schemas[0]  # [Schema(name='default',share='delta_sharing')]

        tables = sharing_client.list_tables(first_schema)
        self.assertEqual(len(tables), 7)

        lending_club: Table = tables[3]

        table_url = sharing_client.table_url(lending_club)

        self.assertTrue(
            (table_url).endswith("delta-sharing/profiles/open-datasets.share#delta_sharing.default.lending_club"))

        spark = TestSharing.spark_session()

        df = ((
            spark.read
            .format("deltaSharing")
            .option("responseFormat", "parquet")
            .option("startingVersion", 1)
            .load(table_url)
        ).select(
            col("loan_amnt"),
            col("funded_amnt"),
            col("term"),
            col("grade"),
            col("home_ownership"),
            col("annual_inc"),
            col("loan_status")
        ))
