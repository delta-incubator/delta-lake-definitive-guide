# Moving Beyond the JVM
> Exploring the Rust Ecosystem and Delta Lake

* [Rust](https://www.rust-lang.org/)
* [Kafka Delta Ingest](https://github.com/delta-io/kafka-delta-ingest)
* [Kafka Delta Ingest - Design Guide](https://github.com/delta-io/kafka-delta-ingest/blob/main/doc/DESIGN.md)
* [Delta RS](https://github.com/delta-io/delta-rs)

# Running the Kafka Delta Ingest Example

If you are running localstack, then you'll need to create the `dldg` bucket.

~~~
aws --endpoint-url=http://localhost:4566 s3api create-bucket \
    --bucket dldg \
    --region us-east-1
~~~

Then copy the contents of the `dldg/ecomm-ingest` to provide a path in the local s3 bucket.

~~~
aws --endpoint-url=http://localhost:4566 s3 cp \
  ./dldg/ s3://dldg/ \
  --recursive
~~~

> Resulting in
> upload: dldg/ecomm-ingest/_delta_log/00000000000000000000.json to > s3://dldg/ecomm-ingest/_delta_log/00000000000000000000.json



Lastly, using the following environment variables, create the empty Delta table.
~~~
export AWS_ENDPOINT_URL=http://0.0.0.0:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_S3_BUCKET=dldg
export TABLE_URI=s3://dldg/ecomm-ingest
~~~

From within the `ch04/rust/kafka-delta-ingest/ecomm-ingest` directory, run the following:
~~~
cargo build && cargo run
~~~

This will result in an empty Delta table for the ecomm workflow.
