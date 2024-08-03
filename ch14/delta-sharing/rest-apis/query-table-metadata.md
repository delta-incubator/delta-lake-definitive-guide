## Using Delta Sharing to Query the Table Metadata
The scope of this request is through the Delta Sharing Clients. This is due to the fact that this endpoint speaks HTTP/2 while the `list-shares`, `get-share`, `list-schemas`, `get-schema`, `list-tables` are all more or less simple REST endpoints with traditional HTTP 1.1 JSON.

