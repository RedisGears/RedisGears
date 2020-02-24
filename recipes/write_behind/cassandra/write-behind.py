from WriteBehind import RGWriteBehind
from WriteBehind.Connectors import CqlConnector, CqlConnection

connection = CqlConnection('cassandra', 'cassandra', '172.17.0.2', 'test')

person_connector = CqlConnector(cqlConnection, 'persons', 'person_id')

person_mappings = {
	'first_name':'first',
	'last_name':'last',
	'age':'age'
}

RGWriteBehind(GB, keysPrefix='person', mappings=person_mappings, connector=person_connector, name='PersonWriteBehind', version='99.99.99')

car_connector = CqlConnector(cqlConnection, 'cars', 'car_id')

car_mappings = {
	'id':'id',
	'color':'color'
}

RGWriteBehind(GB, keysPrefix='car', mappings=car_mappings, connector=car_connector, name='CarsWriteBehind', version='99.99.99')
