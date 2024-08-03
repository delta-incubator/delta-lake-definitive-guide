
## Inspect the Hive Tables 
> connect to the mysql docker, as user dataeng, and use the `metastore` database

```
docker exec -it mysql mysql metastore -u dataeng -p
```

```
mysql> show tables;
+-------------------------------+
| Tables_in_metastore           |
+-------------------------------+
| AUX_TABLE                     |
| BUCKETING_COLS                |
| CDS                           |
| COLUMNS_V2                    |
| COMPACTION_QUEUE              |
| COMPLETED_COMPACTIONS         |
| COMPLETED_TXN_COMPONENTS      |
| CTLGS                         |
| DATABASE_PARAMS               |
| DB_PRIVS                      |
| DBS                           |
| DELEGATION_TOKENS             |
....
74 rows in set (0.00 sec)

mysql> exit
```

## Connect from Local (Outside Docker) into Docker
```
/opt/homebrew/opt/mysql-client@8.0/bin/mysql \
  --host 127.0.0.1 \
  --port 3306 \
  --user dataeng
  --password dataengineering_user
```

## Add more Grants and Be Root on the mysql instance
```
docker exec --user root -it mysql bash -c "mysql -u root -p "
```