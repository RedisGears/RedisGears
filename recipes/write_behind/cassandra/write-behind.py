from WriteBehind import RGWriteBehind
from WriteBehind.Connectors import CqlConnector, CqlConnection

connection = CqlConnection('cassandra', 'cassandra', 'cassandra', 'test')

persons_connector = CqlConnector(connection, 'persons', 'person_id')

persons_mappings = {
	'first_name':'first',
	'last_name':'last',
	'age':'age'
}

RGWriteBehind(GB, keysPrefix='person', mappings=persons_mappings, connector=persons_connector, name='PersonsWriteBehind', version='99.99.99')

cars_connector = CqlConnector(connection, 'cars', 'car_id')

cars_mappings = {
	'id':'id',
	'color':'color'
}

RGWriteBehind(GB, keysPrefix='car', mappings=cars_mappings, connector=cars_connector, name='CarsWriteBehind', version='99.99.99')
