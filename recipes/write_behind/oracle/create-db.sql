
CREATE TABLE test.persons (
  person_id VARCHAR(100) NOT NULL,
  first VARCHAR(100) NOT NULL,
  last VARCHAR(100) NOT NULL,
  age VARCHAR(100) NOT NULL,
  PRIMARY KEY (person_id));

CREATE TABLE test.cars (
  car_id VARCHAR(100) NOT NULL,
  color VARCHAR(100) NOT NULL,
  PRIMARY KEY (car_id));

CREATE TABLE test.persons_exactly_once_table (
  id VARCHAR(100) NOT NULL,
  val VARCHAR(100) NOT NULL,
  PRIMARY KEY (id));
