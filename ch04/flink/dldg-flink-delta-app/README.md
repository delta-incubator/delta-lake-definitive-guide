# Chapter 4: Diving into the Delta Lake Ecosystem
This application provides a simple application that reads from Apache Kafka using Apache Flink and writes to Delta Lake.
In order to compile the application (java), you will need the following.

## Pre-Requisites
* JDK 11 (install manually or `brew install java11`)
* Maven 3.9 (install manually or `brew install mvn`)
* Docker Runtime
* Docker Compose 3.7+ - for running alongside Chapter 5
* [Apache Flink (1.17.1)](https://flink.apache.org/downloads/) 

## Building
~~~
mvn clean verify && mvn package
~~~
> Note: There are some libraries that are marked `<scope>provided</scope>` in the `pom.xml`.
> If you don't have the jars in your local `~/.m2/repository` this can fail the build. It is simple to either:
> a.) comment out the `<scope>provided</scope>`, or b.) search and replace `provided` as `compile`.

# Running the Flink Application
After you have built the local jar (See Building above), you can take the application for a spin in the chapters local
Docker environment.

From the root directory `delta-lake-definitive-guide/`, do the following:

1. Start up the local docker environment by running:
```bash
docker-compose -f /ch04/flink/docker-compose-flink.yaml up
```
or for arm64 chips
```bash
docker-compose -f /ch04/flink/docker-compose-arm64-flink.yaml up
```

2. Submit your Flink application to the live Flink Cluster (Session Mode)
> Tip: Set `$FLINK_HOME` on your local bash/zsh/fish environment. example: `export FLINK_HOME="/path/to/flink-1.17.1"`

~~~
$FLINK_HOME/bin/flink run \
  --detached \
  --jobmanager 127.0.0.1:8888 \
  -c io.delta.dldgv2.ch05.FlinkToDeltaSink \
  --jarfile ./ch04/flink/dldg-flink-delta-app/target/delta-flink-examples.jar \
  app.properties
~~~
The `jobmanager` host:port information points to the `published` port which is `8888`. See the `docker-compose-*` file if
you are unable to connect to Flink.
