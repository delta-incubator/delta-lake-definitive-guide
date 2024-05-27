## List All Tables
This endpoint allows us to list all tables available using our `*.share` profile file, for a unique share. This includes traversing all configured schemas, which in certain cases could be a large number of tables.

**Request**
~~~bash
export DELTA_SHARING_URL="https://sharing.delta.io"
export DELTA_SHARING_PREFIX="delta-sharing"
export DELTA_SHARING_ENDPOINT="$DELTA_SHARING_URL/$DELTA_SHARING_PREFIX"
export BEARER_TOKEN="faaie590d541265bcab1f2de9813274bf233"
export REQUEST_URI="shares/delta_sharing/all-tables"
export REQUEST_URL="$DELTA_SHARING_ENDPOINT/$REQUEST_URI"
export QUERY_PARAMS="maxResults=10"
curl -XGET \
  --header 'Authorization: Bearer $BEARER_TOKEN' \
  --url "$REQUEST_URL?$QUERY_PARAMS"
~~~

**Response**
~~~
{
  "items":[
    {"name":"COVID_19_NYT","schema":"default","share":"delta_sharing"},{"name":"boston-housing","schema":"default","share":"delta_sharing"},{"name":"flight-asa_2008","schema":"default","share":"delta_sharing"},{"name":"lending_club","schema":"default","share":"delta_sharing"},{"name":"nyctaxi_2019","schema":"default","share":"delta_sharing"},{"name":"nyctaxi_2019_part","schema":"default","share":"delta_sharing"},{"name":"owid-covid-data","schema":"default","share":"delta_sharing"}
  ]
}
~~~