from WriteBehind import RGWriteBehind
from WriteBehind.Connectors import OracleSqlConnector, OracleSqlConnection

connection = OracleSqlConnection('test', 'passwd', 'oracle/xe')

persons_connector = OracleSqlConnector(connection, 'persons', 'person_id')

persons_mappings = {
	'first_name':'first',
	'last_name':'last',
	'age':'age'
}

RGWriteBehind(GB, keysPrefix='person', mappings=persons_mappings, connector=persons_connector, name='PersonsWriteBehind', version='99.99.99')

cars_connector = OracleSqlConnector(connection, 'cars', 'car_id')

cars_mappings = {
	'id':'id',
	'color':'color'
}

RGWriteBehind(GB, keysPrefix='car', mappings=cars_mappings, connector=cars_connector, name='CarsWriteBehind', version='99.99.99')
