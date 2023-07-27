from common import gearsTest
from common import TimeLimit
from common import toDictionary
from common import runUntil
from common import runFor
import time

'''
todo:
1. tests for upgrade/delete library (continue from the same id and finishes the pending ids)
'''

@gearsTest()
def testBasicStreamReader(env):
    """#!js api_version=1.0 name=lib
var num_events = 0;
redis.registerFunction("num_events", function(){
    return num_events;
})
redis.registerStreamTrigger("consumer", "stream", function(){
    num_events++;
})
    """
    env.expectTfcall('lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expectTfcall('lib', 'num_events').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expectTfcall('lib', 'num_events').equal(2)

@gearsTest(decodeResponses=False)
def testBasicStreamReaderWithBinaryData(env):
    """#!js api_version=1.0 name=lib
var last_key = null;
var last_key_raw = null;
var last_data = null;
var last_data_raw = null;
redis.registerFunction("stats", function(){
    return [
        last_key,
        last_key_raw,
        last_data,
        last_data_raw
    ];
})
redis.registerStreamTrigger("consumer", new Uint8Array([255]).buffer, function(c, data){
    last_key = data.stream_name;
    last_key_raw = data.stream_name_raw;
    last_data = data.record;
    last_data_raw = data.record_raw;
})
    """
    env.expectTfcall('lib', 'stats').equal([None, None, None, None])
    env.cmd('xadd', b'\xff\xff', '*', b'\xaa', b'\xaa')
    env.expectTfcall('lib', 'stats').equal([None, b'\xff\xff', [[None, None]], [[b'\xaa', b'\xaa']]])

@gearsTest()
def testAsyncStreamReader(env):
    """#!js api_version=1.0 name=lib
var num_events = 0;
redis.registerFunction("num_events", function(){
    return num_events;
})
redis.registerStreamTrigger("consumer", "stream", async function(){
    num_events++;
})
    """
    env.expectTfcall('lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_events'))
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_events'))

@gearsTest()
def testStreamTrim(env):
    """#!js api_version=1.0 name=lib
var num_events = 0;
redis.registerFunction("num_events", function(){
    return num_events;
})
redis.registerStreamTrigger("consumer", "stream", 
    function(){
        num_events++;
    },
    {
        isStreamTrimmed: true
    }
);
    """
    env.expectTfcall('lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expectTfcall('lib', 'num_events').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expectTfcall('lib', 'num_events').equal(2)

@gearsTest(withReplicas=True)
def testSyncStreamTrimWithReplica(env):
    """#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "stream", 
    function(){
        return;
    },
    {
        isStreamTrimmed: true
    }
);
    """
    slave_conn = env.getSlaveConnection()
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('WAIT', '1', '1000').equal(1)
    env.assertEqual(slave_conn.execute_command('xlen', 'stream:1'), 0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('WAIT', '1', '1000').equal(1)
    env.assertEqual(slave_conn.execute_command('xlen', 'stream:1'), 0)

@gearsTest()
def testStreamProccessError(env):
    """#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "stream", function(){
    throw 'Error';
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertEqual('Error', res[0]['stream_triggers'][0]['streams'][0]['last_error'])

@gearsTest()
def testStreamWindow(env):
    """#!js api_version=1.0 name=lib
var promises = [];
redis.registerFunction("num_pending", function(){
    return promises.length;
})

redis.registerFunction("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.registerStreamTrigger("consumer", "stream",
    async function(){
        return await new Promise((resolve, reject) => {
            promises.push(resolve);
        });
    },
    {
        isStreamTrimmed: true,
        window: 3
    }
);
    """
    env.expectTfcall('lib', 'num_pending').equal(0)
    env.expectTfcall('lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.tfcall('lib', 'num_pending'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_triggers'][0]['streams'][0]['pending_ids']))

    env.expect('TFCALL', 'lib.continue', '0').equal('OK')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending'))

    runUntil(env, 2, lambda: len(toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.tfcall('lib', 'num_pending'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_triggers'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runFor(3, lambda: env.tfcall('lib', 'num_pending'))

    env.expect('TFCALL', 'lib.continue', '0').equal('OK')
    runUntil(env, 3, lambda: env.tfcall('lib', 'num_pending'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(2, res[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])

@gearsTest(withReplicas=True)
def testStreamWithReplication(env):
    """#!js api_version=1.0 name=lib
var promises = [];
redis.registerFunction("num_pending", function(){
    return promises.length;
})

redis.registerFunction("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    p = promises[0]
    promises.shift()
    p[1]('continue')
    id = p[0].id;
    return id[0].toString() + "-" + id[1].toString()
})

redis.registerStreamTrigger("consumer", "stream",
    async function(client, data){
        return await new Promise((resolve, reject) => {
            promises.push([data,resolve]);
        });
    },
    {
        isStreamTrimmed: true,
        window: 3
    }
);
    """
    slave_conn = env.getSlaveConnection()

    env.expect('WAIT', '1', '7000').equal(1)

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_pending'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    id_to_read_from = res[0]['stream_triggers'][0]['streams'][0]['id_to_read_from']

    env.expectTfcall('lib', 'continue').equal(id_to_read_from)
    runUntil(env, 1, lambda: len(toDictionary(slave_conn.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams']))
    env.assertEqual(id_to_read_from, toDictionary(slave_conn.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['id_to_read_from'])

    # add 2 more record to the stream
    id1 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    id2 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 2, lambda: slave_conn.execute_command('xlen', 'stream:1'))

    # Turn slave to master
    slave_conn.execute_command('slaveof', 'no', 'one')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending', c=slave_conn))
    def continue_function():
        try:
            return env.tfcall('lib', 'continue', c=slave_conn)
        except Exception as e:
            return str(e)
    runUntil(env, id1, continue_function)
    runUntil(env, id2, continue_function)


@gearsTest()
def testStreamDeletoin(env):
    """#!js api_version=1.0 name=lib
var promises = [];
redis.registerFunction("num_pending", function(){
    return promises.length;
})

redis.registerFunction("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.registerStreamTrigger("consumer", "stream",
    async function(){
        return await new Promise((resolve, reject) => {
            promises.push(resolve);
        });
    },
    {
        isStreamTrimmed: true,
        window: 3
    }
);
    """
    env.expectTfcall('lib', 'num_pending').equal(0)
    env.expectTfcall('lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.tfcall('lib', 'num_pending'))

    env.expect('del', 'stream:1').equal(1)

    env.expectTfcall('lib', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending'))

    env.expectTfcall('lib', 'continue').equal('OK')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_pending'))

    env.expectTfcall('lib', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.tfcall('lib', 'num_pending'))

    res = env.cmd('TFUNCTION', 'LIST', 'vvv')
    env.assertEqual(0, len(toDictionary(res, 6)[0]['stream_triggers'][0]['streams']))

@gearsTest()
def testFlushall(env):
    """#!js api_version=1.0 name=lib
var promises = [];
redis.registerFunction("num_pending", function(){
    return promises.length;
})

redis.registerFunction("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.registerStreamTrigger("consumer", "stream",
    async function(){
        return await new Promise((resolve, reject) => {
            promises.push(resolve);
        });
    },
    {
        isStreamTrimmed: true,
        window: 3
    }
);
    """
    env.expectTfcall('lib', 'num_pending').equal(0)
    env.expectTfcall('lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.tfcall('lib', 'num_pending'))

    env.expect('flushall').equal(True)

    env.expectTfcall('lib', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.tfcall('lib', 'num_pending'))

    env.expectTfcall('lib', 'continue').equal('OK')
    runUntil(env, 1, lambda: env.tfcall('lib', 'num_pending'))

    env.expectTfcall('lib', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.tfcall('lib', 'num_pending'))

    res = env.cmd('TFUNCTION', 'LIST', 'vvv')
    env.assertEqual(0, len(toDictionary(res, 6)[0]['stream_triggers'][0]['streams']))

@gearsTest()
def testMultipleConsumers(env):
    script = """#!js api_version=1.0 name=%s
var promises = [];
redis.registerFunction("num_pending", function(){
    return promises.length;
})

redis.registerFunction("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.registerStreamTrigger("consumer", "stream", 
    async function(){
        return await new Promise((resolve, reject) => {
            promises.push(resolve);
        });
    },
    {
        isStreamTrimmed: true,
        window: 3
    }
)
    """
    env.expect('TFUNCTION', 'LOAD', script % 'lib1').equal('OK')
    env.expect('TFUNCTION', 'LOAD', script % 'lib2').equal('OK')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 1, lambda: env.tfcall('lib2', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 2, lambda: env.tfcall('lib2', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 3, lambda: env.tfcall('lib2', 'num_pending'))

    env.expectTfcall('lib1','continue').equal('OK')
    runUntil(env, 2, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 3, lambda: env.tfcall('lib2', 'num_pending'))
    runFor(3, lambda: env.cmd('XLEN', 'stream:1')) # make sure not trimming

    env.expectTfcall('lib2', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 2, lambda: env.tfcall('lib2', 'num_pending'))
    runUntil(env, 2, lambda: env.cmd('XLEN', 'stream:1'))

    env.expectTfcall('lib1', 'continue').equal('OK')
    env.expectTfcall('lib1', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 2, lambda: env.tfcall('lib2', 'num_pending'))
    runFor(2, lambda: env.cmd('XLEN', 'stream:1')) # make sure not trimming

    env.expectTfcall('lib2', 'continue').equal('OK')
    env.expectTfcall('lib2', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.tfcall('lib1', 'num_pending'))
    runUntil(env, 0, lambda: env.tfcall('lib2', 'num_pending'))
    runUntil(env, 0, lambda: env.cmd('XLEN', 'stream:1'))

@gearsTest()
def testMultipleStreamsForConsumer(env):
    """#!js api_version=1.0 name=lib
var streams = [];

redis.registerFunction("get_stream", function(){
    if (streams.length == 0) {
        throw "No streams"
    }
    let name = streams[0];
    streams.shift();
    return name
})

redis.registerStreamTrigger("consumer", "stream", 
    async function(client, data){
        streams.push(data.stream_name)
    },
    {
        isStreamTrimmed: true,
    }
);
    """

    env.expectTfcall('lib', 'get_stream').error().contains('No streams')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:2', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:2', '*', 'foo', 'bar')

    runUntil(env, 'stream:1', lambda: env.tfcall('lib', 'get_stream'), timeout=1)
    runUntil(env, 'stream:2', lambda: env.tfcall('lib', 'get_stream'), timeout=1)
    runUntil(env, 'stream:1', lambda: env.tfcall('lib', 'get_stream'), timeout=1)
    runUntil(env, 'stream:2', lambda: env.tfcall('lib', 'get_stream'), timeout=1)

@gearsTest()
def testRDBSaveAndLoad(env):
    """#!js api_version=1.0 name=lib

redis.registerStreamTrigger("consumer", "stream", async function(client, data){
    redis.log(data.id);
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 4, lambda: toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    id_to_read_from1 = toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['id_to_read_from']

    env.expect('DEBUG', 'RELOAD').equal('OK')

    id_to_read_from2 = toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['id_to_read_from']

    env.assertEqual(id_to_read_from1, id_to_read_from2)

@gearsTest()
def testSteamReaderPromiseFromSyncFunction(env):
    """#!js api_version=1.0 name=lib

redis.registerStreamTrigger("consumer", "stream", function(client, data){
    return client.callAsync('blpop', 'l', '0');
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    conn = env.getResp3Connection()
    env.assertEqual(0, conn.execute_command('TFUNCTION', 'LIST', 'vvv')[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    env.expect('lpush', 'l', '1').equal(1)
    runUntil(env, 1, lambda: conn.execute_command('TFUNCTION', 'LIST', 'vvv')[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])

@gearsTest()
def testCallingRedisCommandOnStreamConsumer(env):
    """#!js api_version=1.0 name=lib

redis.registerStreamTrigger("consumer", "stream", function(client, data){
    client.call('ping');
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 1, lambda: toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    env.assertEqual(None, toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['last_error'])

@gearsTest()
def testBecomeReplicaWhileProcessingData(env):
    """#!js api_version=1.0 name=lib
var promise = null;
var done = null;

redis.registerAsyncFunction("continue_process", 
    async function(){
        if (promise == null) {
            return "no data to processes";
        }

        var p = new Promise((resume, reject) => {
            done = resume;
        });
        promise("continue");
        promise = null;
        return await p;
    },
    {
        flags: [redis.functionFlags.NO_WRITES]
    }
);

redis.registerStreamTrigger("consumer", "stream",
    async function(client) {
        await new Promise((resume, reject) => {
            promise = resume;
        });
        done("OK");
    },
    {
        isStreamTrimmed: true
    }
)
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 'OK', lambda: env.tfcallAsync('lib', 'continue_process'))
    runUntil(env, 1, lambda: toDictionary(toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 4)[0]['stream_triggers'][0]['streams'][0], 1)['total_record_processed'])

    # Turn into a slave
    env.cmd('slaveof', '127.0.0.1', '3300')
    runUntil(env, 'OK', lambda: env.tfcallAsync('lib', 'continue_process'))
    runUntil(env, 2, lambda: toDictionary(toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 4)[0]['stream_triggers'][0]['streams'][0], 1)['total_record_processed'])

    runFor('no data to processes', lambda: env.tfcallAsync('lib', 'continue_process'))
    res = toDictionary(toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 4)[0]['stream_triggers'][0]['streams'][0], 1)['total_record_processed']
    env.assertEqual(2, res)

    env.cmd('slaveof', 'no', 'one')

@gearsTest()
def testStreamReaderOnAvoidReplicationTraffic(env):
    """#!js api_version=1.0 name=lib
var promise = null;

// we need a key space trigger to register on key miss event
// and continue the run
redis.registerKeySpaceTrigger("consumer", "",
    function(){
        if (promise == null || promise == 1) {
            return "no data to processes";
        }
        promise("continue");
        promise = 1;
        return "OK";
    }
);

redis.registerStreamTrigger("consumer", "stream",
    async function(client) {
        if (promise == null) {
            await new Promise((resume, reject) => {
                promise = resume;
            });
        }
    }
)
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('CLIENT', 'PAUSE', '2000', 'write').equal('OK')
    env.cmd('get', 'x')
    conn = env.getResp3Connection()
    runUntil(env, 2, lambda: conn.execute_command('TFUNCTION', 'LIST', 'vvv')[0]['stream_triggers'][0]['streams'][0]['total_record_processed'], timeout=5)

@gearsTest(withReplicas=True)
def testStreamReaderDeletesStream(env):
    """#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "stream",
    function(client, data) {
        client.call('del', data.stream_name);
    },
    {
        isStreamTrimmed: true
    }
)
    """
    slave_conn = env.getSlaveConnection()
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('exists', 'stream:1').equal(False)
    env.assertEqual(slave_conn.execute_command('exists', 'stream:1'), False)
