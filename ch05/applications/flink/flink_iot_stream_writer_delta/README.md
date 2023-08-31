# Chapter 5: Diving into the Delta Lake Ecosystem
> Apache Flink to Delta Lake

This application provides the building blocks to read and write from Delta Lake using Apache Flink.

## Pre-Requisites
* JDK 11 (install manually or `brew install java11`)
* Maven 3.9 (install manually or `brew install mvn`)

## Building
~~~
mvn clean verify && mvn compile
~~~

## Running
~~~
$FLINK_HOME/bin/flink run \
  --detached \
  --jobmanager 127.0.0.1:8888 \
  --jarfile ./target/delta-flink-examples.jar
~~~