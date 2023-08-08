# Setup Flow
> In order to run the examples in this chapter (5), you will need to setup the docker network named `dldg`.

~~~bash
docker network create dldg
~~~

## Working with Apache Flink
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

Once the Flink service is up and running, you can confirm that things work by going to http://localhost:8081, or http://127.0.0.1:8081. This will show the **Apache Flink Dashboard**.

## Writing to Delta Lake using Flink
Using the _official_ Delta Lake connector for Apache Flink allows us to 
