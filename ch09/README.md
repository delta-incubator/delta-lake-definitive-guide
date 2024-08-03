# Chapter 9: Architecting Yyour Lakehouse
> This chapter references data from the [datasets](../datasets/) directory.

The chapter content here covers the Streaming Medallion Architecture section of Chapter 9. 

## Running the JupyterLab environment
> note: kafka must be running and setup for the jupyter lab contents to work. See the note below under **Kafka**.
~~~
docker compose -f docker-compose-arm64.yaml up
~~~

## Inspiration
The code for Chapter 9 comes from [The Hitchhikers Guide to Delta Lake Streaming](https://github.com/newfront/hitchhikers_guide_to_deltalake_streaming) which was presented at the 2023 Data+AI Summit by [Scott Haines](https://www.linkedin.com/in/scotthaines/) and [Tristen Wentling](https://www.linkedin.com/in/tristen-wentling/).
* [Session Video: YouTube](https://www.youtube.com/watch?v=vTbVBlHhecQ)

## Dataset
This chapter uses the ecommerce dataset from kaggle.com. 

[ECommerce Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).

> Note: If you'd like to explore the entire dataset then you can download it from Kaggle as a follow up to the chapter contents.

## Kafka
> Note: This is the same as in Chapter 4.

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
