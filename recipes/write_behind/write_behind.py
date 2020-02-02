#!/usr/bin/env python

VERSION = '99.99.99'
NAME = 'WriteBehind'

import time
import json

conn = None
sqlText = None
dbtype = None
ConnectionStr = None

def WriteBehindGetConfig(name):
    val = GearsConfigGet(name)
    if val is None:
        raise Exception('%s config value was not given' % name)
    return val

def InitializeParams():
    global dbtype
    global ConnectionStr
    currDbType = WriteBehindGetConfig('%s:dbtype' % NAME)
    if dbtype is not None and currDbType != dbtype:
        Log('warning', '"dbtype" parameter was changed though it can not be modified (Continue running with "dbtype=%s")' % dbtype)
    else:
        dbtype = currDbType

    user = WriteBehindGetConfig('%s:user' % NAME)
    passwd = WriteBehindGetConfig('%s:passwd' % NAME)
    db = WriteBehindGetConfig('%s:db' % NAME)
    account = None

    

    if dbtype == 'mysql':
        ConnectionStr = 'mysql+pymysql://{user}:{password}@{db}'.format(user=user, password=passwd, db=db)
    elif dbtype == 'oracle':
        ConnectionStr = 'mysql+pymysql://{user}:{password}@{db}'.format(user=user, password=passwd, db=db),
    elif dbtype == 'snowflake':
        account = WriteBehindGetConfig('%s:account' % NAME)
        ConnectionStr = 'snowflake://{user}:{password}@{account}/{db}'.format(user=username,
                                                                              password=password,
                                                                              account=account,
                                                                              db=db)
    else:
        raise Exception('given backend not supported')

InitializeParams()

ADD_QUERY_KEY = '_add_query'
DEL_QUERY_KEY = '_delete_query'
TABLE_KEY = '_table'
KEY = '_key'

SLEEP_TIME=1

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

def WriteBehindLog(msg, prefix='%s - ' % NAME, logLevel='notice'):
    msg = prefix + msg
    Log(logLevel, msg)    

def WriteBehindDebug(msg):
    WriteBehindLog(msg, logLevel='debug')

#----------------------------------------------------------------------------------------------

def Connect():
    global ConnectionStr
    from sqlalchemy import create_engine

    WriteBehindLog('Connect: connecting %s, ConnectionStr=%s' % (dbtype, ConnectionStr))
    engine = create_engine(ConnectionStr).execution_options(autocommit=True)
    conn = engine.connect()
    WriteBehindLog('Connect: Connected to ' + dbtype)
    return conn

def PrepereQueries():
    global dbtype

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
        WriteBehindLog('add_query="%s", del_query="%s"' % (v[ADD_QUERY_KEY], v[DEL_QUERY_KEY]))

def GetStreamName(config):
    return '_%s-stream-{%s}' % (config[TABLE_KEY], hashtag())

def CreateStreamInserter(config):
    def AddToStream(r):
        data = []
        data.append([config[KEY], r['key'].split(':')[1]])
        if 'value' in r.keys():
            keys = r['value'].keys()
            for kInHash, kInDB in config.items():
                if kInHash.startswith('_'):
                    continue
                if kInHash not in keys:
                    msg = 'Could not find %s in hash %s' % (kInHash, r['key'])
                    WriteBehindLog(msg)
                    raise Exception(msg)
                data.append([kInDB, r['value'][kInHash]])
        execute('xadd', GetStreamName(config), '*', *sum(data, []))
    return AddToStream

def CreateSQLDataWriter(config):
    def WriteToSQLDB(r):
        # WriteBehindDebug('In WriteToSQLDB')

        global conn
        global sqlText

        if len(r) == 0:
            WriteBehindLog('Warning, got an empty batch')
            return
        for x in r:
            x.pop('streamId', None)## pop the stream id out of the record, we do not need it.
        while True:
            # WriteBehindDebug('WriteToSQLDB: in loop')
            query = None
            errorOccured = False

            try:
                if not conn:
                    from sqlalchemy.sql import text
                    sqlText = text
                    conn = Connect()
            except Exception as e:
                conn = None # next time we will reconnect to the database
                WriteBehindLog('Failed connecting to SQL database, will retry in %d second. error="%s"' % (SLEEP_TIME, str(e)))
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
                WriteBehindLog('Got exception when writing to DB, query="%s", error="%s".' % ((query if query else 'None'), str(e)))
                errorOccured = True

            if errorOccured:
                conn = None # next time we will reconnect to the database
                WriteBehindLog('Error occured while running the sql transaction, will retry in %d second.' % SLEEP_TIME)
                time.sleep(SLEEP_TIME)
                continue # lets retry
            return # we finished successfully, lets break the retry loop

    # WriteBehindDebug('In CreateSQLDataWriter')
    return WriteToSQLDB

def CheckIfHash(r):
    if 'value' not in r.keys() or isinstance(r['value'], dict) :
        return True
    WriteBehindLog('Got a none hash value, key="%s" value="%s"' % (str(r['key']), str(r['value'] if 'value' in r.keys() else 'None')))
    return False

def RegisterExecutions():
    for k, v in config.items():

        regex = k.split(':')[0]

        ## create the execution to write each changed key to stream
        descJson = {
            'name':NAME,
            'version':VERSION,
            'desc':'add each changed key with prefix %s:* to Stream' % regex,
        }
        GB('KeysReader', desc=json.dumps(descJson)).\
        filter(lambda x: x['key'] != GetStreamName(v)).\
        filter(CheckIfHash).\
        foreach(CreateStreamInserter(v)).\
        register(mode='sync', regex='%s:*' % regex, eventTypes=['hset', 'hmset', 'del'])


        ## create the execution to write each key from stream to DB
        descJson = {
            'name':NAME,
            'version':VERSION,
            'desc':'read from stream and write to DB table %s' % v[TABLE_KEY],
        }
        GB('StreamReader', desc=json.dumps(descJson)).\
        aggregate([], lambda a, r: a + [r], lambda a, r: a + r).\
        foreach(CreateSQLDataWriter(v)).\
        count().\
        register(regex='_%s-stream-*' % v[TABLE_KEY], mode="async_local", batch=100, duration=4000, OnRegistered=InitializeParams)

#----------------------------------------------------------------------------------------------

def RegistrationArrToDict(registration, depth):
    if depth >= 2:
        return registration
    if type(registration) is not list:
        return registration
    d = {}
    for i in range(0, len(registration), 2):
        d[registration[i]] = RegistrationArrToDict(registration[i + 1], depth + 1)
    return d

def IsVersionLess(v):
    if VERSION == '99.99.99':
        return True # 99.99.99 is greater then all versions
    major, minor, patch = VERSION.split('.')
    v_major, v_minot, v_patch = v.split('.')

    if int(major) > int(v_major):
        return True
    elif int(major) < int(v_major):
        return False

    if int(minor) > int(v_major):
        return True
    elif int(minor) > int(v_major):
        return False

    if int(patch) > int(v_patch):
        return True
    elif int(patch) > int(v_patch):
        return False

    return False


def UnregisterOldVersions():
    WriteBehindLog('Unregistering old versions of %s' % NAME)
    registrations = execute('rg.dumpregistrations')
    for registration in registrations:
        registrationDict = RegistrationArrToDict(registration, 0)
        descStr = registrationDict['desc']
        try:
            desc = json.loads(descStr)
        except Exception as e:
            continue
        if 'name' in desc.keys() and desc['name'] == NAME:
            if 'version' not in desc.keys():
                execute('rg.unregister', registrationDict['id'])
                WriteBehindLog('Unregistered %s' % registrationDict['id'])
            version = desc['version']
            if IsVersionLess(version):
                execute('rg.unregister', registrationDict['id'])
                WriteBehindLog('Unregistered %s' % registrationDict['id'])
            else:
                raise Exception('Found a version which is greater or equals current version, aborting.')



WriteBehindDebug('-' * 80)
WriteBehindLog('Starting gear')

UnregisterOldVersions()

PrepereQueries()

WriteBehindDebug('-' * 80)
PrintAllQueries()

RegisterExecutions()
WriteBehindDebug('-' * 80)
