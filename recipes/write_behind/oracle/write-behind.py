from WriteBehind import RGWriteBehind
from WriteBehind.Connectors import OracleSqlConnector, OracleSqlConnection

connection = OracleSqlConnection('test', 'passwd', '172.31.51.107/orcl')

person_connector = OracleSqlConnector(connection, 'persons', 'person_id')

person_mappings = {
	'first_name':'first',
	'last_name':'last',
	'age':'age'
}

RGWriteBehind(GB, keysPrefix='person', mappings=person_mappings, connector=person_connector, name='PersonWriteBehind', version='99.99.99')

car_connector = OracleSqlConnector(connection, 'cars', 'car_id')

car_mappings = {
	'id':'id',
	'color':'color'
}

RGWriteBehind(GB, keysPrefix='car', mappings=car_mappings, connector=car_connector, name='CarsWriteBehind', version='99.99.99')
