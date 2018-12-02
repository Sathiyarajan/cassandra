CREATE KEYSPACE dev
WITH replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

CREATE TABLE dev.my_table (
  partition_key TEXT,
  some_timestamp TIMESTAMP,
  PRIMARY KEY (partition_key)
);
INSERT INTO dev.my_table (partition_key, some_timestamp) VALUES ('key', '2015-12-26');

CREATE TABLE dev.test (ID TEXT, NAME TEXT, VALUE TEXT, LAST_MODIFIED_DATE TIMESTAMP,CREATED_DATE timestamp, PRIMARY KEY (ID));
INSERT INTO dev.test (ID, NAME, VALUE, LAST_MODIFIED_DATE) VALUES ('2', 'elephant',  'animal', toTimestamp(now()),toTimestamp(now()));
INSERT INTO dev.test (ID, NAME, VALUE, LAST_MODIFIED_DATE) VALUES ('1', 'orange',  'fruit', toTimestamp(now()), toTimestamp(now()));
