#!/usr/bin/env python

VERSION = '99.99.99'
NAME = 'WriteBehind'

import time
import json

DEFAULT_ON_FAILED_RETRY_INTERVAL = 5
DEFAULT_ACK_EXPIRE_SECONDS = '3600'

OPERATION_DEL_REPLICATE = '~'
OPERATION_DEL_NOREPLICATE = '-'
OPERATION_UPDATE_REPLICATE = '='
OPERATION_UPDATE_NOREPLICATE = '+'
OPERATIONS = [OPERATION_DEL_REPLICATE, OPERATION_DEL_NOREPLICATE, OPERATION_UPDATE_REPLICATE, OPERATION_UPDATE_NOREPLICATE]

conn = None
sqlText = None
dbtype = None
user = None
passwd = None
db = None
account = None
onFailedRetryInterval = None
ConnectionStr = None
ackExpireSeconds = DEFAULT_ACK_EXPIRE_SECONDS
defaultOperation = OPERATION_UPDATE_REPLICATE

def WriteBehindLog(msg, prefix='%s - ' % NAME, logLevel='notice'):
    msg = prefix + msg
    Log(logLevel, msg)

def WriteBehindDebug(msg):
    WriteBehindLog(msg, logLevel='debug')

def WriteBehindGetConfig(name):
    val = GearsConfigGet(name)
    if val is None:
        raise Exception('%s config value was not given' % name)
    return val

def InitializeParams():
    '''
    This function is set on the OnRegistered function of each registration which mean that
    it will be called on each node for each registration.

    Its a good location to initialize global configuration parameters like connection strings, timeouts, policies, and so on.

    Notice that it you put those values here you can change them without re-register the execution (only reload from rdb will do).

    If you have other parameters that can not be change (for example the 'dbtype' and the 'onFailedRetryInterval' in our case)
    Then its a good idea to still check there values and output a log message indicating that those values
    was changed but the change will not take effect.
    '''
    global dbtype
    global user
    global passwd
    global db
    global account
    global ConnectionStr
    global onFailedRetryInterval
    global ackExpireSeconds
    global defaultOperation
    currDbType = GearsConfigGet('%s:dbtype' % NAME)
    if dbtype is not None and currDbType != dbtype:
        WriteBehindLog('"dbtype" parameter was changed though it can not be modified (Continue running with "dbtype=%s")' % dbtype, logLevel='warning')
    else:
        dbtype = currDbType

    currOnFailedRetryInterval = GearsConfigGet('%s:onfailedretryinterval' % NAME, default=DEFAULT_ON_FAILED_RETRY_INTERVAL)
    try:
        currOnFailedRetryInterval = int(currOnFailedRetryInterval)
        if onFailedRetryInterval is not None and currOnFailedRetryInterval != onFailedRetryInterval:
            WriteBehindLog('"onFailedRetryInterval" parameter was changed though it can not be modified (Continue running with "onFailedRetryInterval=%d")' % onFailedRetryInterval, logLevel='warning')
        else:
            onFailedRetryInterval = currOnFailedRetryInterval
    except Exception as e:
        onFailedRetryInterval = DEFAULT_ON_FAILED_RETRY_INTERVAL
        WriteBehindLog('Failed converting "onFailedRetryInterval" to int, running with default "onFailedRetryInterval=%d"' % onFailedRetryInterval, logLevel='warning')

    try:
        user = WriteBehindGetConfig('%s:user' % NAME)
    except Exception:
        if user is None:
            raise
        WriteBehindLog('Can not read user from configuration, will continue using the user which was supplied by the registration initializer.', logLevel='warning')

    try:
        passwd = WriteBehindGetConfig('%s:passwd' % NAME)
    except Exception:
        if passwd is None:
            raise
        WriteBehindLog('Can not read passwd from configuration, will continue using the passwd which was supplied by the registration initializer.', logLevel='warning')

    try:
        db = WriteBehindGetConfig('%s:db' % NAME)
    except Exception:
        if db is None:
            raise
        WriteBehindLog('Can not read db from configuration, will continue using the db which was supplied by the registration initializer.', logLevel='warning')

    ackExpireSeconds = GearsConfigGet('%s:ackexpireseconds' % NAME, default=DEFAULT_ACK_EXPIRE_SECONDS)
    try:
        ackExpireSeconds = int(ackExpireSeconds)
    except Exception as e:
        ackExpireSeconds = DEFAULT_ACK_EXPIRE_SECONDS
        WriteBehindLog('Failed converting "ackExpireSeconds" to int, running with default "ackExpireSeconds=%d"' % ackExpireSeconds, logLevel='warning')

    defaultOperation = GearsConfigGet('%s:defaultoperation' % NAME, default=OPERATION_UPDATE_REPLICATE)
    if defaultOperation not in OPERATIONS:
        defaultOperation = OPERATION_UPDATE_REPLICATE
        WriteBehindLog('Given default operation which is not one of the supported operations (%s) using default "%s"' % (str(OPERATIONS), OPERATION_UPDATE_REPLICATE))

    if dbtype == 'mysql':
        ConnectionStr = 'mysql+pymysql://{user}:{password}@{db}'.format(user=user, password=passwd, db=db)
    elif dbtype == 'oracle':
        ConnectionStr = 'oracle://{user}:{password}@{db}'.format(user=user, password=passwd, db=db)
    elif dbtype == 'snowflake':
        try:
            account = WriteBehindGetConfig('%s:account' % NAME)
        except Exception:
            if account is None:
                raise
            WriteBehindLog('Can not read account from configuration, will continue using the account which was supplied by the registration initializer.', logLevel='warning')

        ConnectionStr = 'snowflake://{user}:{password}@{account}/{db}'.format(user=user,
                                                                              password=passwd,
                                                                              account=account,
                                                                              db=db)
    else:
        raise Exception('given backend not supported')

# Also call the InitializeParams here so we will make sure all the needed params exists.
# Otherwise we will abort
InitializeParams()

ADD_QUERY_KEY = '_add_query'
DEL_QUERY_KEY = '_delete_query'
TABLE_KEY = '_table'
WIRTING_POLICY_KEY = '_writing_policy'
KEY = '_key'
ORIGINAL_KEY = '_original_key'

UUID_KEY = '_uuid'

EXECTLY_ONCE_TABLE_KEY = '_exectly_once_table_key'
EXECTLY_ONCE_UPDATE_QUERY_KEY = '_exectly_once_query_key'
LAST_STREAM_ID_KEY = '_last_stream_id'

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
        EXECTLY_ONCE_TABLE_KEY: "person1_exactly_once_table",
        'first_name': 'first',
        'last_name': 'last',
        'age': 'age',
    },
    'car:license': {
        'color': 'color',
    }
}

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
            def GetUpdateQuery(table, pkey, values_with_pkey, values):
                merge_into = "MERGE INTO %s d USING (SELECT 1 FROM DUAL) ON (d.%s = :%s)" % (table, pkey, pkey)
                not_matched = "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)" % (','.join(values_with_pkey), ','.join([':%s' % a for a in values_with_pkey]))
                matched = "WHEN MATCHED THEN UPDATE SET %s" % (','.join(['%s=:%s' % (a,a) for a in values]))
                query = "%s %s %s" % (merge_into, not_matched, matched)
                return query
            v[ADD_QUERY_KEY] = GetUpdateQuery(v[TABLE_KEY], pkey, values_with_pkey, values)
            if EXECTLY_ONCE_TABLE_KEY in v.keys():
                v[EXECTLY_ONCE_UPDATE_QUERY_KEY] = GetUpdateQuery(v[EXECTLY_ONCE_TABLE_KEY], 'id', ['id', 'val'], ['val'])

        elif dbtype == 'mysql':
            def GetUpdateQuery(table, pkey, vals):
                query = 'REPLACE INTO %s' % table
                values = [val for kk, val in vals if not kk.startswith('_')]
                values = [pkey] + values
                values.sort()
                query = '%s(%s) values(%s)' % (query, ','.join(values), ','.join([':%s' % a for a in values]))
                return query
            v[ADD_QUERY_KEY] = GetUpdateQuery(v[TABLE_KEY], pkey, v.items())
            if EXECTLY_ONCE_TABLE_KEY in v.keys():
                v[EXECTLY_ONCE_UPDATE_QUERY_KEY] = GetUpdateQuery(v[EXECTLY_ONCE_TABLE_KEY], 'id', [('val', 'val')])
        else:
            raise Exception('invalid db type')

        # create delete query
        query = 'delete from %s where %s=:%s' % (v[TABLE_KEY], pkey, pkey)
        v[DEL_QUERY_KEY] = query

def PrintAllQueries():
    for v in config.values():
        WriteBehindLog('add_query="%s"' % (v[ADD_QUERY_KEY]))
        WriteBehindLog('del_query="%s"' % (v[DEL_QUERY_KEY]))
        if EXECTLY_ONCE_UPDATE_QUERY_KEY in v.keys():
            WriteBehindLog('exectly_once_query="%s"' % (v[EXECTLY_ONCE_UPDATE_QUERY_KEY]))

def GetStreamName(config):
    return '_%s-stream-{%s}' % (config[TABLE_KEY], hashtag())

def CompareIds(id1, id2):
    id1_time, id1_num = [int(a) for a in id1.split('-')]
    id2_time, id2_num = [int(a) for a in id2.split('-')]
    if(id1_time > id2_time):
        return 1
    if(id1_time < id2_time):
        return -1

    if(id1_num > id2_num):
        return 1
    if(id1_num < id2_num):
        return -1

    return 0

def CreateSQLDataWriter(config):
    exactlyOnce = True if EXECTLY_ONCE_UPDATE_QUERY_KEY in config.keys() else False
    exactlyOnceLastId = None
    shardId = False
    shouldCompareId = True if exactlyOnce else False
    def WriteToSQLDB(r):
        # WriteBehindDebug('In WriteToSQLDB')
        nonlocal exactlyOnce
        nonlocal exactlyOnceLastId
        nonlocal shardId
        nonlocal shouldCompareId

        global conn
        global sqlText
        global ackExpireSeconds

        if len(r) == 0:
            WriteBehindLog('Warning, got an empty batch')
            return
        query = None

        try:
            if not conn:
                from sqlalchemy.sql import text
                sqlText = text
                conn = Connect()
                if exactlyOnce and not exactlyOnceLastId:
                    shardId = 'shard-%s' % hashtag()
                    result = conn.execute(sqlText('select val from %s where id=:id' % config[EXECTLY_ONCE_TABLE_KEY]), {'id':shardId})
                    res = result.first()
                    if res is not None:
                        exactlyOnceLastId = str(res['val'])
                    else:
                        shouldCompareId = False
        except Exception as e:
            conn = None # next time we will reconnect to the database
            exactlyOnceLastId = None
            shouldCompareId = True if exactlyOnce else False
            msg = 'Failed connecting to SQL database, error="%s"' % str(e)
            WriteBehindLog(msg)
            raise Exception(msg) from None

        idsToAck = []

        trans = conn.begin()
        try:
            batch = []
            # we have only key name, original_key, streamId, it means that the key was deleted
            isAddBatch = True if len(r[0].keys()) > 3 else False
            query = config[ADD_QUERY_KEY] if isAddBatch else config[DEL_QUERY_KEY]
            lastStreamId = None
            for x in r:
                lastStreamId = x.pop('streamId', None)## pop the stream id out of the record, we do not need it.
                if shouldCompareId and CompareIds(exactlyOnceLastId, lastStreamId) >= 0:
                    WriteBehindLog('Skip %s as it was already writen to the backend' % lastStreamId)
                    continue
                shouldCompareId = False
                originalKey = x.pop(ORIGINAL_KEY, None)
                uuid = x.pop(UUID_KEY, None)
                if uuid is not None:
                    idsToAck.append('{%s}%s' % (originalKey, uuid))
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
                if lastStreamId and EXECTLY_ONCE_UPDATE_QUERY_KEY in config.keys():
                    conn.execute(sqlText(config[EXECTLY_ONCE_UPDATE_QUERY_KEY]), {'id':shardId, 'val':lastStreamId})
            trans.commit()
        except Exception as e:
            try:
                trans.rollback()
            except Exception as e:
                WriteBehindLog('Failed rollback transaction')
            conn = None # next time we will reconnect to the database
            exactlyOnceLastId = None
            shouldCompareId = True if exactlyOnce else False
            msg = 'Got exception when writing to DB, query="%s", error="%s".' % ((query if query else 'None'), str(e))
            WriteBehindLog(msg)
            raise Exception(msg) from None

        for idToAck in idsToAck:
            execute('XADD', idToAck, '*', 'status', 'done')
            execute('EXPIRE', idToAck, ackExpireSeconds)

    # WriteBehindDebug('In CreateSQLDataWriter')
    return WriteToSQLDB

def CreateStreamInserter(config):
    def AddToStream(r):
        data = []
        data.append([ORIGINAL_KEY, r['key']])
        data.append([config[KEY], r['key'].split(':')[1]])
        if 'value' in r.keys():
            keys = r['value'].keys()
            if UUID_KEY in keys:
                data.append([UUID_KEY, r['value'][UUID_KEY]])
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

def ShouldProcessHash(r):
    global defaultOperation
    hasValue = 'value' in r.keys()
    operation = defaultOperation
    uuid = ''

    if not hasValue:
        # delete command, use the ~ (delete) operation
        operation = OPERATION_DEL_REPLICATE
    else:
        # make sure its a hash
        if not (isinstance(r['value'], dict)) :
            msg = 'Got a none hash value, key="%s" value="%s"' % (str(r['key']), str(r['value'] if 'value' in r.keys() else 'None'))
            WriteBehindLog(msg)
            raise Exception(msg)


    key = r['key']

    if hasValue:
        value = r['value']
        if '#' in value.keys():
            opVal = value['#']
            if len(opVal) == 0:
                msg = 'Got no operation'
                WriteBehindLog(msg)
                raise Exception(msg)
            operation = value['#'][0]
            if operation not in OPERATIONS:
                msg = 'Got unknown operations "%s"' % operation
                WriteBehindLog(msg)
                raise Exception(msg)
            uuid = value['#'][1:]
            if uuid != '':
                value[UUID_KEY] = uuid
            # delete the # field, we already got the information we need
            value.pop('#', None)
            execute('hdel', key, '#')

    res = True

    if operation == OPERATION_DEL_NOREPLICATE:
        # we need to just delete the key but delete it directly will cause
        # key unwanted key space notification so we need to rename it first
        newKey = '__{%s}__' % key
        execute('RENAME', key, newKey)
        execute('DEL', newKey)
        res = False

    if operation == OPERATION_UPDATE_NOREPLICATE:
        res = False

    if not res and uuid != '':
        # no replication to backend is needed but ack is require
        '{%s}%s' % (key, uuid)
        execute('XADD', idToAck, '*', 'status', 'done')
        execute('EXPIRE', idToAck, ackExpireSeconds)

    return res

def RegisterExecutions():
    global onFailedRetryInterval
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
        filter(ShouldProcessHash).\
        foreach(CreateStreamInserter(v)).\
        register(mode='sync', regex='%s:*' % regex, eventTypes=['hset', 'hmset', 'del'], onRegistered=InitializeParams)


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
        register(regex='_%s-stream-*' % v[TABLE_KEY],
                 mode="async_local",
                 batch=100,
                 duration=4000,
                 onRegistered=InitializeParams,
                 onFailedPolicy="retry",
                 onFailedRetryInterval=onFailedRetryInterval)

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
    WriteBehindLog('Unregistered old versions')



WriteBehindDebug('-' * 80)
WriteBehindLog('Starting gear')

UnregisterOldVersions()

PrepereQueries()

WriteBehindDebug('-' * 80)
PrintAllQueries()

RegisterExecutions()
WriteBehindDebug('-' * 80)
