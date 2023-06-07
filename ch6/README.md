# DLDG: Chapter 6: Maintaining your Delta Lake
> Note: The chapter exercises and notebooks are currently a work in progress.

## Datasets
* [Covid-19 NYC Dataset](https://github.com/delta-io/delta-docs/tree/main/static/quickstart_docker/rs/data/COVID-19_NYT)


## Using the Notebook
> Note: the notebook runs using the `delta_quickstart` Docker image.

~~~
export DLDG_CHAPTER_DIR=~/path/to/delta-lake-definitive-guide/ch6
docker run --rm -it \
  --name delta_quickstart \
  -v $DLDG_CHAPTER_DIR/:/opt/spark/work-dir/ch6 \
  -p 8888-8889:8888-8889 \
  delta_quickstart:0.9.0
~~~

Once the docker environment spins up, you'll see the following: 
~~~
To access the server, open this file in a browser:
        file:///home/NBuser/.local/share/jupyter/runtime/jpserver-7-open.html
    Or copy and paste one of these URLs:
        http://127.0.0.1:8888/lab?token=UNIQUE_TOKEN
~~~

You can follow along in the book and the lab using the url.

### Alternative Datasets
[Vegetable and Fruit Prices](https://www.kaggle.com/datasets/vislupus/vegetable-and-fruit-prices?resource=download)