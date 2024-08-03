## Table Version
Knowing the version of the Delta table can be incredibly useful when the Delta Sharing service is being used by any internal or external data team. This capability enables us to understand how frequently changes have occurred between the last run of a given pipeline or other equally important workflow. By caching the last known version of a specific Delta table, we can know if our data is stale simply by hitting this simple endpoint.

> Note: This is not implemented at the time of writing on the https://sharing.delta.io endpoint.

**Request**
~~~bash
export DELTA_SHARING_URL="https://sharing.delta.io"
export DELTA_SHARING_PREFIX="delta-sharing"
export DELTA_SHARING_ENDPOINT="$DELTA_SHARING_URL/$DELTA_SHARING_PREFIX"
export BEARER_TOKEN="faaie590d541265bcab1f2de9813274bf233"

export REQUEST_URI="shares/delta_sharing/schemas/default/tables/lending_club/version"
export REQUEST_URL="$DELTA_SHARING_ENDPOINT/$REQUEST_URI"

curl \
  --http2 \
  --header 'Authorization: Bearer $BEARER_TOKEN' \
  --url "$REQUEST_URL"
~~~

**Response**
~~~bash
HTTP/2 200
delta-table-version: 123
~~~