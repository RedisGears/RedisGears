
CREATE KEYSPACE test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE test.persons (
	person_id text,
	first text,
	last text,
	age text,
	PRIMARY KEY (person_id)
);

CREATE TABLE test.cars (
	car_id text,
	color text,
	PRIMARY KEY (car_id)
);

CREATE TABLE test.persons_exactly_once_table (
	id text,
	val text,
	PRIMARY KEY (id)
);
