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
    """#!js name=lib
var num_events = 0;
redis.register_function("num_events", function(){
    return num_events;
})
redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    num_events++;
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(2)

@gearsTest()
def testAsyncStreamReader(env):
    """#!js name=lib
var num_events = 0;
redis.register_function("num_events", function(){
    return num_events;
})
redis.register_stream_consumer("consumer", "stream", 1, false, async function(){
    num_events++;
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_events'))
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_events'))

@gearsTest()
def testStreamTrim(env):
    """#!js name=lib
var num_events = 0;
redis.register_function("num_events", function(){
    return num_events;
})
redis.register_stream_consumer("consumer", "stream", 1, true, function(){
    num_events++;
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(2)

@gearsTest()
def testStreamProccessError(env):
    """#!js name=lib
redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    throw 'Error';
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vv'), 6)
    env.assertEqual('Error', res[0]['stream_consumers'][0]['streams'][0]['last_error'])

@gearsTest()
def testStreamWindow(env):
    """#!js name=lib
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(){
    return await new Promise((resolve, reject) => {
        promises.push(resolve);
    });
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_pending').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_consumers'][0]['streams'][0]['pending_ids']))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    
    runUntil(env, 2, lambda: len(toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_consumers'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runFor(3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(2, res[0]['stream_consumers'][0]['streams'][0]['total_record_processed'])

@gearsTest(envArgs={'useSlaves': True})
def testStreamWithReplication(env):
    """#!js name=lib
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    p = promises[0]
    promises.shift()
    p[1]('continue')
    id = p[0].id;
    return id[0].toString() + "-" + id[1].toString()
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(client, data){
    return await new Promise((resolve, reject) => {
        promises.push([data,resolve]);
    });
})
    """
    slave_conn = env.getSlaveConnection()

    env.expect('WAIT', '1', '7000').equal(1)

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    id_to_read_from = res[0]['stream_consumers'][0]['streams'][0]['id_to_read_from']

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal(id_to_read_from)
    runUntil(env, 1, lambda: len(toDictionary(slave_conn.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams']))
    env.assertEqual(id_to_read_from, toDictionary(slave_conn.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['id_to_read_from'])

    # add 2 more record to the stream
    id1 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    id2 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 2, lambda: slave_conn.execute_command('xlen', 'stream:1'))

    # Turn slave to master
    slave_conn.execute_command('slaveof', 'no', 'one')
    runUntil(env, 2, lambda: slave_conn.execute_command('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    def continue_function():
        try:
            return slave_conn.execute_command('RG.FUNCTION', 'CALL', 'lib', 'continue')
        except Exception as e:
            return str(e)
    runUntil(env, id1, continue_function)
    runUntil(env, id2, continue_function)


@gearsTest()
def testStreamDeletoin(env):
    """#!js name=lib
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(){
    return await new Promise((resolve, reject) => {
        promises.push(resolve);
    });
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_pending').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('del', 'stream:1').equal(1)

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = env.cmd('RG.FUNCTION', 'LIST', 'vvv')
    env.assertEqual(0, len(toDictionary(res, 6)[0]['stream_consumers'][0]['streams']))

@gearsTest()
def testFlushall(env):
    """#!js name=lib
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(){
    return await new Promise((resolve, reject) => {
        promises.push(resolve);
    });
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_pending').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('flushall').equal(True)

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = env.cmd('RG.FUNCTION', 'LIST', 'vvv')
    env.assertEqual(0, len(toDictionary(res, 6)[0]['stream_consumers'][0]['streams']))

@gearsTest()
def testMultipleConsumers(env):
    script = """#!js name=%s
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(){
    return await new Promise((resolve, reject) => {
        promises.push(resolve);
    });
})
    """
    env.expect('RG.FUNCTION', 'LOAD', script % 'lib1').equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', script % 'lib2').equal('OK')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib1', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))
    runFor(3, lambda: env.cmd('XLEN', 'stream:1')) # make sure not trimming

    env.expect('RG.FUNCTION', 'CALL', 'lib2', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))
    runUntil(env, 2, lambda: env.cmd('XLEN', 'stream:1'))

    env.expect('RG.FUNCTION', 'CALL', 'lib1', 'continue').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib1', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))
    runFor(2, lambda: env.cmd('XLEN', 'stream:1')) # make sure not trimming

    env.expect('RG.FUNCTION', 'CALL', 'lib2', 'continue').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib2', 'continue').equal('OK')
    runUntil(env, 0, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib1', 'num_pending'))
    runUntil(env, 0, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib2', 'num_pending'))
    runUntil(env, 0, lambda: env.cmd('XLEN', 'stream:1'))

@gearsTest()
def testMultipleStreamsForConsumer(env):
    """#!js name=lib
var streams = [];

redis.register_function("get_stream", function(){
    if (streams.length == 0) {
        throw "No streams"
    }
    let name = streams[0];
    streams.shift();
    return name
})

redis.register_stream_consumer("consumer", "stream", 1, true, async function(client, data){
    streams.push(data.stream_name)
})
    """

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get_stream').error().contains('No streams')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:2', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:2', '*', 'foo', 'bar')

    runUntil(env, 'stream:1', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'get_stream'), timeout=1)
    runUntil(env, 'stream:2', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'get_stream'), timeout=1)
    runUntil(env, 'stream:1', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'get_stream'), timeout=1)
    runUntil(env, 'stream:2', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'get_stream'), timeout=1)

@gearsTest()
def testRDBSaveAndLoad(env):
    """#!js name=lib

redis.register_stream_consumer("consumer", "stream", 1, false, async function(client, data){
    redis.log(data.id);
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 4, lambda: toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['total_record_processed'])
    id_to_read_from1 = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['id_to_read_from']

    env.expect('DEBUG', 'RELOAD').equal('OK')

    id_to_read_from2 = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['id_to_read_from']

    env.assertEqual(id_to_read_from1, id_to_read_from2)

@gearsTest()
def testCallingRedisCommandOnStreamConsumer(env):
    """#!js name=lib

redis.register_stream_consumer("consumer", "stream", 1, false, function(client, data){
    client.call('ping');
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    
    runUntil(env, 1, lambda: toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['total_record_processed'])
    env.assertEqual('None', toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['last_error'])

@gearsTest()
def testBecomeReplicaWhileProcessingData(env):
    """#!js name=lib
var promise = null;
var done = null;

redis.register_function("continue_process", async function(){
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
['no-writes'])

redis.register_stream_consumer("consumer", "stream", 1, true, async function(client) {
    await new Promise((resume, reject) => {
        promise = resume;
    });
    done("OK");
}) 
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 'OK', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'continue_process'))
    runUntil(env, 1, lambda: toDictionary(toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 4)[0]['stream_consumers'][0]['streams'][0], 1)['total_record_processed'])

    # Turn into a slave
    env.cmd('slaveof', '127.0.0.1', '3300')
    runUntil(env, 'OK', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'continue_process'))
    runUntil(env, 2, lambda: toDictionary(toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 4)[0]['stream_consumers'][0]['streams'][0], 1)['total_record_processed'])
    
    runFor('no data to processes', lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'continue_process'))
    res = toDictionary(toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 4)[0]['stream_consumers'][0]['streams'][0], 1)['total_record_processed']
    env.assertEqual(2, res)

    env.cmd('slaveof', 'no', 'one')
