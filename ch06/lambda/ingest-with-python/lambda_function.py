#!/usr/bin/env python3

from deltalake import DeltaTable, write_deltalake
import json
import os
import pyarrow as pa

def schema():
    return pa.schema([('id', pa.int32()), ('name', pa.string())])

def lambda_handler(event, context):
    status = 400
    table_url = os.environ['TABLE_URL']
    body = json.dumps({'message' : 'The request was invalid, oops!'})

    if event and event['requestContext']['http']['method'] == 'POST':
        try:
            input = pa.RecordBatch.from_pylist(json.loads(event['body']))
            dt = DeltaTable(table_url)
            write_deltalake(dt, data=input, schema=schema(), mode='append')
            status = 201
            body = json.dumps({'message' : 'Thanks for the data!'})
        except Exception as err:
            print(event)
            print(f'Failed to handle request: {err}')
            print(err)
            status = 400
            body = json.dumps({'message' : str(err), 'type' : type(err).__name__})

    return {
        'statusCode' : status,
        'headers' : {'Content-Type' : 'application/json'},
        'isBase64Encoded' : False,
        'body' : body,
    }


if __name__ == '__main__':
    import json
    print(json.dumps(lambda_handler(None, None), indent=4))

