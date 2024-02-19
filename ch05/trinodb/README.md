# Trino
> Requires the Hive Metastore + Object Store (using MinIO or bring your own s3 or other store to the table)

[Delta Lake: Plugin](https://github.com/trinodb/trino/tree/426/plugin/trino-delta-lake)

## Running Trino using Docker

> Notes: The version of trino at the time of writing is `426`.
> * docker pull trinodb/trino:426-arm64
> * docker pull trinodb/trino:426-amd64
> * apache/hive: 3.1.3 and 4.0.0 is in beta2 (at time of writing)

* [Docker Docs](https://trino.io/docs/current/installation/containers.html)

Getting things to run locally require setting up MySQL and MinIO in order to run a local Hive Metastore.

1. Beginning in the trinodb directory of ch05. `cd ch05/trinodb`.
2. Start up MinIO and MySQL. `docker compose -f docker-compose-mysql-minio.yaml up -d`
3. Then you'll need to bootstrap **Hive 3.1** for MySQL. There is a [README.md](./metastore/README.md) provided for just that.
4. Now you can start the `metastore` container. `docker compose -f docker-compose-metastore.yaml up`. Note the [`IS_RESUME="true"`](./docker-compose-metastore.yaml) environment variable in the metastore. This flag skips the bootstrap process which you ran manually in step 3.
5. Launch `trino` with `docker compose -f docker-compose-trino.yaml` or `docker-compose-trino-arm64.yaml`.

Start using `trino` natively with Delta Lake. See Using Trino and Delta Lake or follow along in chapter 5.

## Using Trino and Delta Lake

> docker exec -it trinodb trino
~~~
trino> show catalogs;
Catalog
---------
 delta
 jmx
 memory
 system
 tpcds
 tpch
(6 rows)
~~~

# Create a Schema
> this is also referred to as a `database` in the old hive land
~~~
trino> create schema delta.bronze_schema;
CREATE SCHEMA
~~~

If you get an exception: It is going to be based on Permissions. The permissions will be associated with the 
aws.access.key
aws.secret.key

for your IAM user
~~~
Query 20231001_182856_00004_zjwqg failed: Got exception: java.nio.file.AccessDeniedException s3a://com.newfront.dldgv2/delta/bronze_schema.db: getFileStatus on s3a://com.newfront.dldgv2/delta/bronze_schema.db: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403; Error Code: 403 Forbidden; Request ID: 4CTZ4YYYRBGCNVD5; S3 Extended Request ID: 59n0G+uPobL2euFNZU9V/zfAdxlctef5iXG+ohGrpcI5o8syYAF+mfVJs8BG7jGiUDJaziM47FwHY4AEDiOrOA==), S3 Extended Request ID: 59n0G+uPobL2euFNZU9V/zfAdxlctef5iXG+ohGrpcI5o8syYAF+mfVJs8BG7jGiUDJaziM47FwHY4AEDiOrOA==:403 Forbidden
~~~

Fix the problem with modified IAM permissions
~~~
> follow up by updating the IAM policy to include the missing `getFileStatus`.
- If you are going to production, don't provide all access like you'll see below!
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::arn:aws:s3:::com.newfront.dldgv2"
        }
    ]
}
```
~~~


## Show Schemas
~~~
trino> show schemas from delta;

Schema
--------------------
 default
 information_schema
 bronze_schema
(3 rows)
~~~

Using the ecommerce row data from the Flink Connector
* https://trino.io/docs/current/connector/delta-lake.html#type-mapping

| delta | trino |
|---    |---    |
| BOOLEAN      |   BOOLEAN    |
| INTEGER      |   INTEGER    |
| BYTE      |   TINYINT    |
| SHORT     |  SMALLINT    |
| LONG      |  BIGINT      |
| FLOAT     |  REAL        |
| DOUBLE    |  DOUBLE      |
| DECIMAL(p,s) | DECIMAL(p,s) |
| STRING    | VARCHAR  |
| BINARY  |  VARBINARY |
| DATE    |  DATE  |
| TIMESTAMPNTZ (TIMESTAMP_NTZ) | TIMESTAMP(6) |
| TIMESTAMP | TIMESTAMP(3) WITH TIME ZONE |
| ARRAY | ARRAY |
| MAP | MAP |
| STRUCT(...) | ROW(...) |

## Show Tables
~~~
trino> show tables from delta.bronze_schema;
 Table
-------
(0 rows)
~~~

## Create Table in the Schema
~~~
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
~~~

Create the Table

## Table Options
| property name | description |
| ---           | ---         |
| location      | file system location URI for table |
| partitioned_by | set of columns to partition on |
| checkpoint_interval | how often the check for changes on the table |
| change_data_feed_enabled | track changes made to the table for use in CDC/CDF applications |
| column_mapping_mode | how to map the underlying parquet columns: options (id, name, none)


1. `trino> use delta.bronze_schema;`
~~~
CREATE TABLE ecomm_v1_clickstream (
  event_date DATE,
  event_time VARCHAR(255),
  event_type VARCHAR(255),
  product_id INTEGER,
  category_id BIGINT,
  category_code VARCHAR(255),
  brand VARCHAR(255),
  price DECIMAL(5,2),
  user_id INTEGER,
  user_session VARCHAR(255)
)
WITH (
    partitioned_by = ARRAY['event_date'],
    checkpoint_interval = 30,
    change_data_feed_enabled = false,
    column_mapping_mode = 'name'
)
~~~

## View the Created Table
~~~
trino:bronze_schema> show tables;

Table
----------------------
 ecomm_v1_clickstream
(1 row)
~~~

## Describe the Table
~~~
trino:bronze_schema> describe ecomm_v1_clickstream;

    Column     |     Type     | Extra | Comment
---------------+--------------+-------+---------
 event_date    | date         |       |
 event_time    | varchar      |       |
 event_type    | varchar      |       |
 product_id    | integer      |       |
 category_id   | bigint       |       |
 category_code | varchar      |       |
 brand         | varchar      |       |
 price         | decimal(5,2) |       |
 user_id       | integer      |       |
 user_session  | varchar      |       |
(10 rows)
~~~

## Insert Records
~~~
trino:bronze_schema> INSERT INTO bronze_schema.ecomm_v1_clickstream
    VALUES
        (DATE '2023-10-01', '2023-10-01T19:10:05.704396Z', 'view', 44600062, 2103807459595387724, 'health.beauty', 'nars', 35.79, 541312140, '72d76fde-8bb3-4e00-8c23-a032dfed738c'),
        (DATE('2023-10-01'), '2023-10-01T19:20:05.704396Z', 'view', 54600062, 2103807459595387724, 'health.beauty', 'lancome', 122.79, 541312140, '72d76fde-8bb3-4e00-8c23-a032dfed738c');
~~~

~~~
INSERT: 2 rows
~~~

## Select Statement
~~~
trino> select * from delta.bronze_schema.ecomm_v1_clickstream;
 event_date |         event_time          | event_type | product_id |     category_id     | category_code |  brand  | price  |  user_id  |             user_session
------------+-----------------------------+------------+------------+---------------------+---------------+---------+--------+-----------+--------------------------------------
 2023-10-01 | 2023-10-01T19:10:05.704396Z | view       |   44600062 | 2103807459595387724 | health.beauty | nars    |  35.79 | 541312140 | 72d76fde-8bb3-4e00-8c23-a032dfed738c
 2023-10-01 | 2023-10-01T19:20:05.704396Z | view       |   54600062 | 2103807459595387724 | health.beauty | lancome | 122.79 | 541312140 | 72d76fde-8bb3-4e00-8c23-a032dfed738c
(2 rows)
~~~



Create a Table AS
~~~
...
~~~

~~~
WITH (
  location = 's3://my-bucket/a/path',
  partitioned_by = ARRAY['regionkey'],
  checkpoint_interval = 5,
  change_data_feed_enabled = false,
  column_mapping_mode = 'name'
)
~~~

## DROP TABLE
~~~
DROP TABLE ecomm_v1_clickstream;
~~~

### Concurrency
```
delta.enable-non-concurrent-writes=true
```

## Register a Table
The connector can register table into the metastore with existing transaction logs and data files.
- the external location is now deprecated in Trino
```
delta.register-table-procedure.enabled=true
```

The system.register_table procedure allows the caller to register an existing Delta Lake table in the metastore, using its existing transaction logs and data files:
~~~
CALL delta.system.register_table(schema_name => 'bronze_db', table_name => 'ecomm_original', table_location => 's3://my-bucket/a/path')
To prevent unauthorized users from accessing data, this procedure is disabled by default. The procedure is enabled only when delta.register-table-procedure.enabled is set to true.
~~~

## Table Operations

### VACUUM 
```
delta.vacuum.min-retention=14d
```

`CALL delta.system.vacuum('bronze_schema', 'ecomm_v1_clickstream', '14d');`

### OPTIMIZE
> compaction
~~~
ALTER TABLE delta.bronze_schema.ecomm_v1_clickstream EXECUTE optimize;
~~~

> ignore files less than 10MB
~~~
ALTER TABLE test_table EXECUTE optimize(file_size_threshold => '10MB')
~~~

> only optimize by specific partition
~~~
ALTER TABLE test_partitioned_table EXECUTE optimize
WHERE partition_key = 1
~~~

### Metadata Tables

Metadata tables#
The connector exposes several metadata tables for each Delta Lake table. These metadata tables contain information about the internal structure of the Delta Lake table. You can query each metadata table by appending the metadata table name to the table name:


## Table History
You can retrieve the changelog of the Delta Lake table test_table by using the following query:

~~~
trino> select * from delta.bronze_schema."ecomm_v1_clickstream$history";
 version |          timestamp          | user_id | user_name |  operation   |         operation_parameters          |    cluster_id     | read_version |  isolation_level  | is_blind_append
---------+-----------------------------+---------+-----------+--------------+---------------------------------------+-------------------+--------------+-------------------+-----------------
       0 | 2023-10-01 19:47:35.618 UTC | trino   | trino     | CREATE TABLE | {queryId=20231001_194733_00055_7b59h} | trino-426-trinodb |            0 | WriteSerializable | true
       1 | 2023-10-01 19:48:41.212 UTC | trino   | trino     | WRITE        | {queryId=20231001_194838_00057_7b59h} | trino-426-trinodb |            0 | WriteSerializable | true
(2 rows)
~~~

## Table Functions
* https://trino.io/docs/current/connector/delta-lake.html#table-functions

### CDC - query changes

~~~
trino> SELECT * FROM TABLE(delta.system.table_changes(schema_name => 'bronze_schema', table_name => 'ecomm_v1_clickstream', since_version => 0));

 event_date |         event_time          | event_type | product_id |     category_id     | category_code |  brand  | price  |  user_id  |             user_session             | _change_type | _commit_version |      _commit_timestamp
------------+-----------------------------+------------+------------+---------------------+---------------+---------+--------+-----------+--------------------------------------+--------------+-----------------+-----------------------------
 2023-10-01 | 2023-10-01T19:10:05.704396Z | view       |   44600062 | 2103807459595387724 | health.beauty | nars    |  35.79 | 541312140 | 72d76fde-8bb3-4e00-8c23-a032dfed738c | insert       |               1 | 2023-10-01 19:48:41.212 UTC
 2023-10-01 | 2023-10-01T19:20:05.704396Z | view       |   54600062 | 2103807459595387724 | health.beauty | lancome | 122.79 | 541312140 | 72d76fde-8bb3-4e00-8c23-a032dfed738c | insert       |               1 | 2023-10-01 19:48:41.212 UTC
(2 rows)
~~~

## Auth
`delta.security` - follow up with governance chapter

## Executing SQL Commands via the Trino CLI
> With the Trino docker image up and running

```
docker exec -it trinodb trino
```

* Trino Catalogs (connectors) are located in `/etc/trino/`.
* Trino Plugins (used for connectors) are located in `/usr/lib/trino/plugins`


## Trino Delta Connector
> https://trino.io/docs/current/connector/delta-lake.html

> Java Version (openjdk 17.0.8.1+1) in container


