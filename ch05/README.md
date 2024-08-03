# DLDG: Chapter 5: Maintaining your Delta Lake
> Note: The chapter exercises and notebooks are currently a work in progress.

## Using the Notebook
> Note: the notebook runs using the `delta_quickstart` Docker image.

### Running Without Docker Compose
~~~
export DLDG_CHAPTER_DIR=~/path/to/delta-lake-definitive-guide/ch05
docker run --rm -it \
  --name delta_quickstart \
  -v $DLDG_CHAPTER_DIR/:/opt/spark/work-dir/ch05 \
  -p 8888-8889:8888-8889 \
  deltaio/delta-docker:latest
~~~

## Running with Docker Compose
> Note: If you are running on M1/M2 apple silicon: Use the `-arm64` compose file.

### Spin up the local Environment
**For x86_64 cpu**
~~~
docker compose -f docker-compose-ch05.yaml up
~~~

**For Apple Silicon or ARM support**
~~~
docker compose -f docker-compose-ch05-arm64.yaml up
~~~

Once the docker environment spins up, you'll see the following: 
~~~
To access the server, open this file in a browser:
        file:///home/NBuser/.local/share/jupyter/runtime/jpserver-7-open.html
    Or copy and paste one of these URLs:
        http://127.0.0.1:8888/lab?token=UNIQUE_TOKEN
~~~

and if you've run the compose command using `-d` for detached mode, you can run `docker logs deltalake-quickstart` to get the jupyter lab url.

Using the jupyter lab url, authenticate, and then go to `http://127.0.0.1:8888/lab/tree/ch05` to follow along with the chapter.

### Spin down the local Environment
> Note: if you've started the environment using `docker compose ... -d` then you will need to use the following command.
~~~
docker compose -f docker-compose-ch05.yaml down --remove-orphans
~~~


## Datasets
* [Covid-19 NYC Dataset](https://github.com/delta-io/delta-docs/tree/main/static/quickstart_docker/rs/data/COVID-19_NYT)

### Alternative Datasets
[Vegetable and Fruit Prices](https://www.kaggle.com/datasets/vislupus/vegetable-and-fruit-prices?resource=download)

### Building the Docker Image Locally
[Delta Docs: Quickstart Docker](https://github.com/delta-io/delta-docs/blob/main/static/quickstart_docker/README.md)