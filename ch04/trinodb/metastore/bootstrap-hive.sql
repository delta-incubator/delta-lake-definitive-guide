CREATE DATABASE `metastore`;

/* CREATE the dataeng user */
CREATE USER 'dataeng'@'%' IDENTIFIED BY 'dataengineering_user';
CREATE USER 'dataeng'@'localhost' IDENTIFIED BY 'dataengineering_user';

/* Apply User Grants on our dataeng user */
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'dataeng'@'%';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'dataeng'@'localhost';
GRANT ALL PRIVILEGES ON `default`.* TO 'dataeng'@'%';
GRANT ALL PRIVILEGES ON `metastore`.* TO 'dataeng'@'%';
GRANT ALL PRIVILEGES ON `metastore`.* TO 'dataeng'@'localhost';
FLUSH PRIVILEGES;

USE `metastore`;

SOURCE /hive-schema-3.1.0.mysql.sql
