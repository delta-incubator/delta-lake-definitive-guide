#!/usr/bin/env python3

from lambda_function import lambda_handler, schema

from deltalake import DeltaTable
import json
import os
import pyarrow as pa
import pytest

@pytest.fixture
def empty_table_url(tmp_path):
    os.environ['TABLE_URL'] = str(tmp_path)
    return tmp_path

@pytest.fixture
def delta_table_url(tmp_path):
    table_url = str(tmp_path)
    os.environ['TABLE_URL'] = table_url
    DeltaTable.create(table_url, schema=schema())
    return table_url

def test_empty(empty_table_url):
    result = lambda_handler(None, None)
    assert result['statusCode'] == 400

def test_empty_body(empty_table_url):
    event = {
        'requestContext' : {
            'http': {
                'method' : 'GET',
            },
        },
        'body' : ''
    }
    result = lambda_handler(event, None)
    assert result['statusCode'] == 400

def test_json_body(delta_table_url):
    event = {
        'requestContext' : {
            'http': {
                'method' : 'POST',
            },
        },
        'body' : json.dumps([
            {'name' : 'Denny', 'id' : 0}
        ])
    }
    result = lambda_handler(event, None)
    assert result['statusCode'] == 201, result['body']

    dt = DeltaTable(delta_table_url)
    assert len(dt.files()) == 1, 'No data was added to the table!'

def test_invalid_json_body(empty_table_url):
    event = {
        'requestContext' : {
            'http': {
                'method' : 'POST',
            },
        },
        'body' : 'junk!'
    }
    result = lambda_handler(event, None)
    assert result['statusCode'] == 400
    assert len(result['body']) > 0
    body = json.loads(result['body'])
    assert body['type'] == 'JSONDecodeError'
