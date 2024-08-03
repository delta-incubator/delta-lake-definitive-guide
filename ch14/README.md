# Using Delta Sharing
> The contents of this directory correspond to Chapter 14 in the Delta Lake Definitive Guide

Under the directory `delta-sharing`, there is a python application that shows you how to use Delta Sharing.

## Apache Spark
> Include the Delta Sharing binaries
~~~
$SPARK_HOME/bin/spark-shell --packages io.delta:delta-sharing-spark_2.12:3.1.0
~~~

~~~scala
import org.apache.spark.sql.functions.{col}

val dldg_path = "/path/to/book/github"
val profile_file_location = f"$dldg_path/ch04/delta-sharing/open-datasets.share"

val delta_share_table_url = f"$profile_file_location#delta_sharing.default.owid-covid-data"

val df = spark.read.format("deltaSharing").load(delta_share_table_url).select("iso_code", "location", "date").where(col("iso_code").equalTo("USA"))

df.show()
~~~

~~~
+--------+-------------+----------+
|iso_code|     location|      date|
+--------+-------------+----------+
|     USA|United States|2020-01-22|
|     USA|United States|2020-01-23|
|     USA|United States|2020-01-24|
|     USA|United States|2020-01-25|
|     USA|United States|2020-01-26|
|     USA|United States|2020-01-27|
|     USA|United States|2020-01-28|
|     USA|United States|2020-01-29|
|     USA|United States|2020-01-30|
|     USA|United States|2020-01-31|
|     USA|United States|2020-02-01|
|     USA|United States|2020-02-02|
|     USA|United States|2020-02-03|
|     USA|United States|2020-02-04|
|     USA|United States|2020-02-05|
|     USA|United States|2020-02-06|
|     USA|United States|2020-02-07|
|     USA|United States|2020-02-08|
|     USA|United States|2020-02-09|
|     USA|United States|2020-02-10|
+--------+-------------+----------+
~~~

~~~
val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
val df = spark.readStream.format("deltaSharing")
  .option("startingVersion", "1")
  .option("skipChangeCommits", "true")
  .load(tablePath)
~~~