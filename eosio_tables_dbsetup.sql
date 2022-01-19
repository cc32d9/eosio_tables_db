CREATE DATABASE eosio_tables;

CREATE USER 'eosio_tables'@'localhost' IDENTIFIED BY 'Ohch3ook';
GRANT ALL ON eosio_tables.* TO 'eosio_tables'@'localhost';
grant SELECT on eosio_tables.* to 'eosio_tables_ro'@'%' identified by 'eosio_tables_ro';

