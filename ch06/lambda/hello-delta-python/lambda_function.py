#!/usr/bin/env python3

import os

def lambda_handler(event, context):
    from deltalake import DeltaTable
    url = os.environ['TABLE_URL']
    dt = DeltaTable(url)
    metadata = dt.metadata()
    return {
        'version' : dt.version(),
        'table' : url,
        'files' : dt.files(),
    'metadata' : {
            'name' : metadata.name,
            'created_time' : metadata.created_time,
            'id' : metadata.id,
            'description' : metadata.description,
            'partition_columns' : metadata.partition_columns,
            'configuration' : metadata.configuration,
        },
    }


if __name__ == '__main__':
    import json
    print(json.dumps(lambda_handler(None, None), indent=4))

