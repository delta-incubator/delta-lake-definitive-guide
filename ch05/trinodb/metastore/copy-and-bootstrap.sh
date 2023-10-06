
# Installs and bootstraps Hive for a fresh start
docker cp "${PWD}/bootstrap.sh" "mysql:/"
docker cp "${PWD}/bootstrap-hive.sql" "mysql:/"
docker cp "${PWD}/hive-schema-3.1.0.mysql.sql" "mysql:/"

# setup some initial stuff
docker exec mysql ./bootstrap.sh

# bootstrap hive (requires dataengineering password)
# see `docker-compose-mysql-minio.yaml` for the full name
echo "Installing Hive Tables. You will be prompted for the MySQL Root Password. (dat**nginer***)"
docker exec --user root -it mysql bash -c "mysql -u root -p < bootstrap-hive.sql"
