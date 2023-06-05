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
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(2)

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
    env.expect('TFCALL', 'lib', 'stats', '0').equal([None, None, None, None])
    env.cmd('xadd', b'\xff\xff', '*', b'\xaa', b'\xaa')
    env.expect('TFCALL', 'lib', 'stats', '0').equal([None, b'\xff\xff', [[None, None]], [[b'\xaa', b'\xaa']]])

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
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_events', '0'))
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_events', '0'))

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
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('TFCALL', 'lib', 'num_events', '0').equal(2)

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
    env.expect('TFCALL', 'lib', 'num_pending', '0').equal(0)
    env.expect('TFCALL', 'lib', 'continue', '0').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_triggers'][0]['streams'][0]['pending_ids']))

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    runUntil(env, 2, lambda: len(toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_triggers'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runFor(3, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

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
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    id_to_read_from = res[0]['stream_triggers'][0]['streams'][0]['id_to_read_from']

    env.expect('TFCALL', 'lib', 'continue', '0').equal(id_to_read_from)
    runUntil(env, 1, lambda: len(toDictionary(slave_conn.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams']))
    env.assertEqual(id_to_read_from, toDictionary(slave_conn.execute_command('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['id_to_read_from'])

    # add 2 more record to the stream
    id1 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    id2 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 2, lambda: slave_conn.execute_command('xlen', 'stream:1'))

    # Turn slave to master
    slave_conn.execute_command('slaveof', 'no', 'one')
    runUntil(env, 2, lambda: slave_conn.execute_command('TFCALL', 'lib', 'num_pending', '0'))
    def continue_function():
        try:
            return slave_conn.execute_command('TFCALL', 'lib', 'continue', '0')
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
    env.expect('TFCALL', 'lib', 'num_pending', '0').equal(0)
    env.expect('TFCALL', 'lib', 'continue', '0').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('del', 'stream:1').equal(1)

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 0, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

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
    env.expect('TFCALL', 'lib', 'num_pending', '0').equal(0)
    env.expect('TFCALL', 'lib', 'continue', '0').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('flushall').equal(True)

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

    env.expect('TFCALL', 'lib', 'continue', '0').equal('OK')
    runUntil(env, 0, lambda: env.cmd('TFCALL', 'lib', 'num_pending', '0'))

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
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 1, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))

    env.expect('TFCALL', 'lib1', 'continue', '0').equal('OK')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 3, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))
    runFor(3, lambda: env.cmd('XLEN', 'stream:1')) # make sure not trimming

    env.expect('TFCALL', 'lib2', 'continue', '0').equal('OK')
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))
    runUntil(env, 2, lambda: env.cmd('XLEN', 'stream:1'))

    env.expect('TFCALL', 'lib1', 'continue', '0').equal('OK')
    env.expect('TFCALL', 'lib1', 'continue', '0').equal('OK')
    runUntil(env, 0, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 2, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))
    runFor(2, lambda: env.cmd('XLEN', 'stream:1')) # make sure not trimming

    env.expect('TFCALL', 'lib2', 'continue', '0').equal('OK')
    env.expect('TFCALL', 'lib2', 'continue', '0').equal('OK')
    runUntil(env, 0, lambda: env.cmd('TFCALL', 'lib1', 'num_pending', '0'))
    runUntil(env, 0, lambda: env.cmd('TFCALL', 'lib2', 'num_pending', '0'))
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

    env.expect('TFCALL', 'lib', 'get_stream', '0').error().contains('No streams')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:2', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:2', '*', 'foo', 'bar')

    runUntil(env, 'stream:1', lambda: env.cmd('TFCALL', 'lib', 'get_stream', '0'), timeout=1)
    runUntil(env, 'stream:2', lambda: env.cmd('TFCALL', 'lib', 'get_stream', '0'), timeout=1)
    runUntil(env, 'stream:1', lambda: env.cmd('TFCALL', 'lib', 'get_stream', '0'), timeout=1)
    runUntil(env, 'stream:2', lambda: env.cmd('TFCALL', 'lib', 'get_stream', '0'), timeout=1)

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
    runUntil(env, 'OK', lambda: env.cmd('TFCALLASYNC', 'lib', 'continue_process', '0'))
    runUntil(env, 1, lambda: toDictionary(toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 4)[0]['stream_triggers'][0]['streams'][0], 1)['total_record_processed'])

    # Turn into a slave
    env.cmd('slaveof', '127.0.0.1', '3300')
    runUntil(env, 'OK', lambda: env.cmd('TFCALLASYNC', 'lib', 'continue_process', '0'))
    runUntil(env, 2, lambda: toDictionary(toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 4)[0]['stream_triggers'][0]['streams'][0], 1)['total_record_processed'])

    runFor('no data to processes', lambda: env.cmd('TFCALLASYNC', 'lib', 'continue_process', '0'))
    res = toDictionary(toDictionary(env.execute_command('TFUNCTION', 'LIST', 'vvv'), 4)[0]['stream_triggers'][0]['streams'][0], 1)['total_record_processed']
    env.assertEqual(2, res)

    env.cmd('slaveof', 'no', 'one')
