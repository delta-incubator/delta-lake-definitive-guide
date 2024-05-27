## List Tables
This endpoint allows us to view the tables available using our `*.share` profile file, for a unique share as well as a specific pointer to the schema for which we wish to view available tables.

**Request**
~~~bash
export DELTA_SHARING_URL="https://sharing.delta.io"
export DELTA_SHARING_PREFIX="delta-sharing"
export DELTA_SHARING_ENDPOINT="$DELTA_SHARING_URL/$DELTA_SHARING_PREFIX"
export BEARER_TOKEN="faaie590d541265bcab1f2de9813274bf233"
export REQUEST_URI="shares/delta_sharing/schemas/default/tables"
export REQUEST_URL="$DELTA_SHARING_ENDPOINT/$REQUEST_URI"
export QUERY_PARAMS="maxResults=4"
curl \
  --request GET \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer $BEARER_TOKEN' \
  --url "$REQUEST_URL?$QUERY_PARAMS"
~~~

**Response**
~~~bash
{
    "items":[
        {"name":"COVID_19_NYT","schema":"default","share":"delta_sharing"},{"name":"boston-housing","schema":"default","share":"delta_sharing"},{"name":"flight-asa_2008","schema":"default","share":"delta_sharing"},
        {"name":"lending_club","schema":"default","share":"delta_sharing"}
    ],
    "nextPageToken":"CgE0Eg1kZWx0YV9zaGFyaW5nGgdkZWZhdWx0"
}
~~~

**Request for Next Page**
~~~bash
export QUERY_PARAMS="maxResults=4&nextPageToken=CgE0Eg1kZWx0YV9zaGFyaW5nGgdkZWZhdWx0"
curl \
  --request GET \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer $BEARER_TOKEN' \
  --url "$REQUEST_URL?$QUERY_PARAMS"
~~~

**Paginated Response**
~~~bash
{
  "items":[
    {"name":"nyctaxi_2019","schema":"default","share":"delta_sharing"},{"name":"nyctaxi_2019_part","schema":"default","share":"delta_sharing"},{"name":"owid-covid-data","schema":"default","share":"delta_sharing"}
  ]
}
~~~