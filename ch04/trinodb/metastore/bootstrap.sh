#!/bin/bash

bootstrap_sql="${PWD}/bootstrap.sql"

user=${MYSQL_USER}
password=${MYSQL_PASSWORD}
db_root_pw=${MYSQL_ROOT_PASSWORD}

cat > /secrets <<EOL
[client]
user=${user}
password=${password}
EOL

cat > /secrets_root <<EOL
[client]
user=root
password=${password}
EOL

mysql --defaults-file=${PWD}/secrets < ${bootstrap_sql}