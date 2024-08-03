## Get Share
This endpoint allows you to view the schemas associated with a unique share. Remember that everything is predicated on a specific `*.share` profile file.

## API Request to the Get Share endpoint
~~~bash
export DELTA_SHARING_URL="https://sharing.delta.io"
export DELTA_SHARING_PREFIX="delta-sharing"
export DELTA_SHARING_ENDPOINT="$DELTA_SHARING_URL/$DELTA_SHARING_PREFIX"
export BEARER_TOKEN="faaie590d541265bcab1f2de9813274bf233"
export REQUEST_URI="shares/delta_sharing"
export REQUEST_URL="$DELTA_SHARING_ENDPOINT/$REQUEST_URI"
curl -XGET \
  --header 'Authorization: Bearer $BEARER_TOKEN' \
  --url "$REQUEST_URL"
~~~

## Response from the Get Share endpoint
~~~bash
{"share":{"name":"delta_sharing"}}
~~~