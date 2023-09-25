# Setup Flow
> In order to run the examples in this chapter (5), you will need to setup the docker network named `dldg`.

~~~bash
docker network create dldg
~~~

## Working with the Apache Flink Delta Lake Connector
To follow along with the book, you will need an Apache Flink cluster. To run things locally, you can start spin up a Flink cluster using one of the two _docker compose_ templates (x86_64 or arm64). 
> If you are unsure of your CPU type, take a look at the note `Finding your CPU type` below.

~~~bash
docker compose -f docker-compose-flink.yaml up
~~~

~~~bash
docker compose -f docker-compose-arm64-flink.yaml up 
~~~

> Finding your CPU type: figuring out the cpu type for your environment can be done with `uname -a`. You will see `arm64` if you are running an arm based cpu.

---

Once the Flink service is up and running, you can confirm that things work by going to http://localhost:8888, or http://127.0.0.1:8888. 
This will show us the **Apache Flink Dashboard**.

## Bootstrapping the Environment

1. Create the Kafka Topic `ecomm.v1.clickstream`
This will enable us to publish records that we can consume with Apache Flink, as well as `delta-kafka-ingest` later on in the chapter.

~~~
docker exec -it kafka-rp \
  rpk topic create ecomm.v1.clickstream --brokers=localhost:9092
~~~

2. View the new Kafka Topic
~~~
docker exec -it kafka-rp \
    rpk topic list
~~~

Output will look similar
~~~
NAME                  PARTITIONS  REPLICAS
_schemas              1           1
ecomm.v1.clickstream  1           1
~~~

3. Produce some Records to the `ecomm.v1.clickstream` topic
~~~
docker exec -it kafka-rp \
 rpk topic produce ecomm.v1.clickstream --brokers=localhost:9092
~~~

```
{"event_time": "2023-08-30T00:00:00Z","event_type": "view","product_id": 4782,"category_id": 2053013552326770905,"category_code": "appliances.environment.water_heater","brand": "heater3","price": 2789.0,"user_id": 195,"user_session": "19ae88e1-4a02-4b57-94a8-a46f6c6c60c4"}
```
```
{"event_time": "2023-09-21T00:00:00Z","event_type": "view","product_id": 4783,"category_id": 2051113552326770905,"category_code": "appliances.televisions","brand": "sony","price": 2789.0,"user_id": 196,"user_session": "19ae88e1-4a02-4b57-94a8-a46f6c6c60c4"}
```

4. When you want to remove the topic, or just start over
```
docker exec -it kafka-rp \
  rpk topic delete ecomm.v1.clickstream
```

## Writing to Delta Lake using Flink
Using the _official_ Delta Lake connector for Apache Flink allows us to 


# Trino
> Requires the Hive Metastore + Object Store (using MinIO or bring your own s3 or other store to the table)

[Delta Lake: Plugin](https://github.com/trinodb/trino/tree/426/plugin/trino-delta-lake)


## Running Trino using Docker

> Notes: The version of trino at the time of writing is `426`.
> * docker pull trinodb/trino:426-arm64
> * docker pull trinodb/trino:426-amd64
> * apache/hive: 3.1.3 and 4.0.0 is in beta2 (at time of writing)

[Hive Standalone Metastore](https://hub.docker.com/r/apache/hive)

* [Docker Docs](https://trino.io/docs/current/installation/containers.html)

** Launch the Trino docker using the following**
```
docker-compose -f ch05/docker-compose-trino.yaml
```
or the following for Apple Silicon or ARM based cpus
```
docker-compose -f ch05/docker-compose-trino-arm64.yaml
```

## Executing SQL Commands via the Trino CLI
> With the docker image up and running

```
docker exec -it trinodb trino
```

> To launch with custom configs: --volume $PWD/etc:/etc/trino trinodb/trino

```
catalogs: -> /etc/trino/
plugins: -> /usr/lib/trino/plugins
```

## Trino Delta Connector
> https://trino.io/docs/current/connector/delta-lake.html

> Java Version (openjdk 17.0.8.1+1) in container

