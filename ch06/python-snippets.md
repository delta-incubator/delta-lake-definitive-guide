# Python snippets

This file contains the code snippets from the chapter in a format that can be
easily doctested with `make test`


## Getting Started

```
>>> from deltalake import DeltaTable
>>> dt = DeltaTable('./data/deltatbl-partitioned')
>>> dt.files()
['c2=foo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet', 'c2=foo1/part-00000-786c7455-9587-454f-9a4c-de0b22b62bbd.c000.snappy.parquet', 'c2=foo0/part-00001-ca647ee7-f1ad-4d70-bf02-5d1872324d6f.c000.snappy.parquet', 'c2=foo1/part-00001-1c702e73-89b5-465a-9c6a-25f7559cd150.c000.snappy.parquet']
>>> df = dt.to_pandas()
>>> df
   c1	 c2
0   0  foo0
1   2  foo0
2   4  foo0
3   1  foo1
4   3  foo1
5   6  foo0
6   8  foo0
7   5  foo1
8   7  foo1
9   9  foo1
>>>
```

### Reading large data sets

```
>>> dt.to_pandas(partitions=[('c2', '=', 'foo0')])
   c1	 c2
0   0  foo0
1   2  foo0
2   4  foo0
3   6  foo0
4   8  foo0
>>>
```

```
>>> dt.files([('c2', '=', 'foo0')])
['c2=foo0/part-00000-2bcc9ff6-0551-4401-bd22-d361a60627e3.c000.snappy.parquet', 'c2=foo0/part-00001-ca647ee7-f1ad-4d70-bf02-5d1872324d6f.c000.snappy.parquet']
>>>
```

```
>>> dt.to_pandas(partitions=[('c2', '=', 'foo0')], columns=['c1'])
   c1
0   0
1   2
2   4
3   6
4   8
>>>
```


```
>>> dt.to_pandas(partitions=[('c2', '=', 'foo0')], columns=['c1'], filters=[('c1', '<=', 4), ('c1', '>', 0)])
   c1
0   2
1   4
>>>
```


#### Generating a table with interesting file statistics

This example will use the CO2 data referened below to generate a partitioned table with only a few records per `.parquet` file to demonstrate file statistics usage.

```
>>> import pandas as pa
>>> from deltalake import DeltaTable, write_deltalake
>>> df = pa.read_csv('data/co2_mm_mlo.csv', comment='#')
>>> len(df) > 0
True
>>> write_deltalake('data/gen/filestats', data=df, partition_by=['year'], max_rows_per_file=4, max_rows_per_group=4, min_rows_per_group=1)
>>>
```

```
>>> from deltalake import DeltaTable
>>> dt = DeltaTable('./data/gen/filestats')
>>> len(dt.files())
198
>>> df = dt.to_pandas(filters=[('year', '=', 2022), ('month', '>=', 9)])
>>> len(df)
4
>>>
```


### Writing data

```
>>> import pandas as pd
>>> df = pd.read_csv('./data/co2_mm_mlo.csv', comment='#')
>>> len(df) > 0
True
>>> from deltalake import write_deltalake, DeltaTable
>>> write_deltalake('./data/gen/co2_monthly', df)
>>> dt = DeltaTable('./data/gen/co2_monthly')
>>> len(dt.files()) > 0
True
>>> df = dt.to_pandas()
>>> len(df) > 0
True
>>>
```

With partitioning

```
>>> df = pd.read_csv('./data/co2_mm_mlo.csv', comment='#')
>>> len(df) > 0
True
>>> write_deltalake('./data/gen/co2_monthly_partitioned', data=df, partition_by=['year'])
>>>
```


###  Going beyond Pandas

```
>>> df = pd.read_csv('./data/co2_mm_mlo.csv', comment='#')
>>> write_deltalake('./data/gen/co2_monthly_partitioned', data=df, mode='overwrite', partition_by=['year'])
>>> 
```


