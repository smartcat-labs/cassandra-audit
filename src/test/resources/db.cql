CREATE KEYSPACE cassandra_audit_test WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
USE cassandra_audit_test;

CREATE TABLE auditable_entity (
    key text,
    PRIMARY KEY (key)
);

CREATE TABLE column_exclusion_entity (
	key text,
	col1 text,
	col2 text,
	column3 text,
	PRIMARY KEY (key)
);