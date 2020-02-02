#!/usr/bin/env python

import time

engine = None
conn = None
sqlText = None
dbtype = None
_debug=True

# addQuery =
#   MERGE INTO table d USING (SELECT 1 FROM DUAL) ON (d.pkey_col = 'pkey')
#   WHEN NOT MATCHED THEN INSERT (pkey_col, col2) VALUES ('pkey', 'v2')
#   WHEN MATCHED THEN UPDATE SET col2='v2'

# deleteQuery = 'delete from test_table where test_table.key="%s"'

ADD_QUERY_KEY = '_add_query'
DEL_QUERY_KEY = '_delete_query'
TABLE_KEY = '_table'
KEY = '_key'

SLEEP_TIME=1

#----------------------------------------------------------------------------------------------
# Database configuration

## see https://docs.sqlalchemy.org/en/13/core/engines.html for more info

MYSQL_CONFIG = {
    'ConnectionStr': 'mysql+pymysql://{user}:{password}@{db}'.format(user='test', password='passwd', db='mysql/test'),
}

ORACLE_CONFIG = {
    'ConnectionStr': 'oracle://{user}:{password}@{db}'.format(user='test', password='passwd', db='oracle/xe'),
}

def get_snowflake_conn_str():
    import configparser
    c = configparser.ConfigParser()
    c.read('/opt/redislabs/.snowsql/config')
    username = c['connections']['username']
    password = c['connections']['password']
    account = c['connections']['accountname']
    return 'snowflake://{user}:{password}@{account}/{db}'.format(
        user=username,
        password=password,
        account=account,
        db='test')

SNOWFLAKE_CONFIG = {
    'ConnectionStr': get_snowflake_conn_str(),
}

DATABASES = {
    'snowflake': SNOWFLAKE_CONFIG,
    'oracle': ORACLE_CONFIG,
    'mysql': MYSQL_CONFIG,
}

#----------------------------------------------------------------------------------------------
# Key mapping

# config = {
#     'redis-hash-key:pkey-column': {
#         TABLE_KEY: 'sql-table-override', # otherwise, use 'redis-hash-key'
#         'redis-hash-field': 'sql-column',
#     },
# }

config = {
    'person2:id': {
        TABLE_KEY: 'person1',
        'first_name': 'first',
        'last_name': 'last',
        'age': 'age',
    },
    'car:license': {
        'color': 'color',
    },
}

#----------------------------------------------------------------------------------------------

def Log(msg, prefix='RedisGears - '):
    msg = prefix + msg
    try:
        execute('debug', 'log', msg)
    except Exception:
        print(msg)

def Debug(msg, prefix='RedisGears - '):
    if not _debug:
        return
    msg = prefix + msg
    try:
        execute('debug', 'log', msg)
    except Exception:
        print(msg)

#----------------------------------------------------------------------------------------------

def Connect():
    global conn
    global engine
    global dbtype
    global sqlText
    from sqlalchemy import create_engine

    if dbtype is None:
        Log('Connect: determining dbtype')
        for type, config in DATABASES.items():
            connstr = config['ConnectionStr']
            Log('Connect: trying conntecting %s, ConnectionStr=%s' % (type, connstr))
            try:
                engine1 = create_engine(connstr).execution_options(autocommit=True)
                conn1 = engine1.connect()
                Log('DB detected: %s' % (type))
                dbtype = type
                conn1.close()
                return None
            except:
                Debug('Connect: no ' + type)
        Log('Connect: Cannot determine DB engine')
        raise Exception('Connect: Cannot determine DB engine')

    if dbtype in DATABASES:
        config = DATABASES[dbtype]
        connstr = config['ConnectionStr']
        Log('Connect: connecting %s, ConnectionStr=%s' % (dbtype, connstr))
        engine = create_engine(connstr).execution_options(autocommit=True)
        conn = engine.connect()
        Log('Connect: Connected to ' + dbtype)
        return conn
    else:
        Log('Connect: invalid db engine: ' + dbtype)
        raise Exception('Connect: invalid db engine: ' + dbtype)

    return None

def PrepereQueries():
    global conn
    global dbtype

    # Log('Determining dbtype')
    # Connect() # determine dbtype

    for k,v in config.items():
        table, pkey = k.split(':')
        if TABLE_KEY not in v.keys():
            v[TABLE_KEY] = table
        v[KEY] = pkey
        if v[TABLE_KEY] is None or pkey is None:
            raise Exception('failed to create query for %s', str(k))

        # create upsert query
        if dbtype == 'oracle' or dbtype == 'snowflake':
            values = [val for kk, val in v.items() if not kk.startswith('_')]
            values_with_pkey = [pkey] + values
            merge_into = "MERGE INTO %s d USING (SELECT 1 FROM DUAL) ON (d.%s = :%s)" % (v[TABLE_KEY], pkey, pkey)
            not_matched = "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (','.join(values_with_pkey), ','.join([':%s' % a for a in values_with_pkey]))
            matched = "WHEN MATCHED THEN UPDATE SET %s" % (','.join(['%s=:%s' % (a,a) for a in values]))
            query = "%s %s %s" % (merge_into, not_matched, matched)
        elif dbtype == 'mysql':
            query = 'REPLACE INTO %s' % v[TABLE_KEY]
            values = [val for kk, val in v.items() if not kk.startswith('_')]
            values = [pkey] + values
            values.sort()
            query = '%s(%s) values(%s)' % (query, ','.join(values), ','.join([':%s' % a for a in values]))
        else:
            raise Exception('invalid db type')

        v[ADD_QUERY_KEY] = query

        # create delete query
        query = 'delete from %s where %s=:%s' % (v[TABLE_KEY], pkey, pkey)
        v[DEL_QUERY_KEY] = query

def PrintAllQueries():
    for v in config.values():
        Log('add_query="%s", del_query="%s"' % (v[ADD_QUERY_KEY], v[DEL_QUERY_KEY]))

def GetStreamName(config):
    return '_%s-stream-{%s}' % (config[TABLE_KEY], hashtag())

def CreateStreamInserter(config):
    def AddToStream(r):
        # Debug('In AddToStream: ' + r['key'])
        data = []
        data.append([config[KEY], r['key'].split(':')[1]])
        if 'value' in r.keys():
            keys = r['value'].keys()
            for kInHash, kInDB in config.items():
                if kInHash.startswith('_'):
                    continue
                if kInHash not in keys:
                    msg = 'Could not find %s in hash %s' % (kInHash, r['key'])
                    Log(msg)
                    raise Exception(msg)
                data.append([kInDB, r['value'][kInHash]])
        execute('xadd', GetStreamName(config), '*', *sum(data, []))
    # Debug('In CreateStreamInserter')
    return AddToStream

def CreateSQLDataWriter(config):
    def WriteToSQLDB(r):
        # Debug('In WriteToSQLDB')

        global conn
        global sqlText

        if len(r) == 0:
            Log('Warning, got an empty batch')
            return
        for x in r:
            x.pop('streamId', None)## pop the stream id out of the record, we do not need it.
        while True:
            # Debug('WriteToSQLDB: in loop')
            query = None
            errorOccured = False

            try:
                if not conn:
                    from sqlalchemy.sql import text
                    sqlText = text
                    conn = Connect()
            except Exception as e:
                conn = None # next time we will reconnect to the database
                Log('Failed connecting to SQL database, will retry in %d second. error="%s"' % (SLEEP_TIME, str(e)))
                time.sleep(SLEEP_TIME)
                continue # lets retry

            try:
                batch = []
                isAddBatch = True if len(r[0].keys()) > 1 else False # we have only key name, it means that the key was deleted
                query = config[ADD_QUERY_KEY] if isAddBatch else config[DEL_QUERY_KEY]
                for x in r:
                    if len(x.keys()) == 1: # we have only key name, it means that the key was deleted
                        if isAddBatch:
                            conn.execute(sqlText(query), batch)
                            batch = []
                            isAddBatch = False
                            query = config[DEL_QUERY_KEY]
                        batch.append(x)
                    else:
                        if not isAddBatch:
                            conn.execute(sqlText(query), batch)
                            batch = []
                            isAddBatch = True
                            query = config[ADD_QUERY_KEY]
                        batch.append(x)
                if len(batch) > 0:
                    conn.execute(sqlText(query), batch)
            except Exception as e:
                Log('Got exception when writing to DB, query="%s", error="%s".' % ((query if query else 'None'), str(e)))
                errorOccured = True

            if errorOccured:
                conn = None # next time we will reconnect to the database
                Log('Error occured while running the sql transaction, will retry in %d second.' % SLEEP_TIME)
                time.sleep(SLEEP_TIME)
                continue # lets retry
            return # we finished successfully, lets break the retry loop

    # Debug('In CreateSQLDataWriter')
    return WriteToSQLDB

def CheckIfHash(r):
    if 'value' not in r.keys() or isinstance(r['value'], dict) :
        return True
    Log('Got a none hash value, key="%s" value="%s"' % (str(r['key']), str(r['value'] if 'value' in r.keys() else 'None')))
    return False

def RegisterExecutions():
    for k, v in config.items():
        regs0 = execute('rg.dumpregistrations')

        regex = k.split(':')[0]
        ## create the execution to write each changed key to stream
        GB('KeysReader', desc='add each changed key with prefix %s:* to Stream' % regex).\
        filter(lambda x: x['key'] != GetStreamName(v)).\
        filter(CheckIfHash).\
        foreach(CreateStreamInserter(v)).\
        register(mode='sync', regex='%s:*' % regex)

        regs1 = execute('rg.dumpregistrations')
        if len(regs0) == len(regs1):
            Log("Reader failed to register: k=%s v=%s" % (k, str(v)))

        ## create the execution to write each key from stream to DB
        GB('StreamReader', desc='read from stream and write to DB table %s' % v[TABLE_KEY]).\
        aggregate([], lambda a, r: a + [r], lambda a, r: a + r).\
        foreach(CreateSQLDataWriter(v)).\
        count().\
        register(regex='_%s-stream-*' % v[TABLE_KEY], mode="async_local", batch=100, duration=4000)

        regs2 = execute('rg.dumpregistrations')
        if len(regs1) == len(regs2):
            Log("Writer failed to register: k=%s v=%s" % (k, str(v)))

    # Debug('-' * 80)
    # regs = execute('rg.dumpregistrations')
    # Debug('regs: ' + str(regs))
    # Debug('-' * 80)

#----------------------------------------------------------------------------------------------

Debug('-' * 80)
Log('Starting gear')

PrepereQueries()

Debug('-' * 80)
PrintAllQueries()

RegisterExecutions()
Debug('-' * 80)
