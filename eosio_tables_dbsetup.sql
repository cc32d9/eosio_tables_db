CREATE DATABASE eosio_tables;

CREATE USER 'eosio_tables'@'localhost' IDENTIFIED BY 'Ohch3ook';
GRANT ALL ON eosio_tables.* TO 'eosio_tables'@'localhost';
grant SELECT on eosio_tables.* to 'eosio_tables_ro'@'%' identified by 'eosio_tables_ro';

use eosio_tables;

CREATE TABLE SYNC
(
 network           VARCHAR(15) PRIMARY KEY,
 block_num         BIGINT NOT NULL,
 block_time        DATETIME NOT NULL,
 irreversible      BIGINT NOT NULL
) ENGINE=InnoDB;


CREATE TABLE CONTRACT_LAST_UPD
(
 network           VARCHAR(15) NOT NULL,
 contract          VARCHAR(13) NOT NULL,
 block_num         BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX CONTRACT_LAST_UPD_I01 ON CONTRACT_LAST_UPD(network, contract);



CREATE TABLE WATCH_CONTRACTS
(
 network           VARCHAR(15) NOT NULL,
 contract          VARCHAR(13) NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX WATCH_CONTRACTS_I01 ON WATCH_CONTRACTS(network, contract);


CREATE TABLE EXPORT_TBL_UPDATES
(
 network           VARCHAR(15) NOT NULL,
 contract          VARCHAR(13) NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX EXPORT_TBL_UPDATES_I01 ON EXPORT_TBL_UPDATES(network, contract);
