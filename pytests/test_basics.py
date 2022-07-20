from common import gearsTest
from common import toDictionary
from common import runUntil

'''
todo:
1. tests for rdb save and load
'''

@gearsTest()
def testBasicJSInvocation(env):
    """#!js name=foo
redis.register_function("test", function(){
    return 1
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)

@gearsTest()
def testCommandInvocation(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return client.call('ping')
})  
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal('PONG')

@gearsTest()
def testLibraryUpgrade(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return 1
})  
    """
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})  
    '''
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(2)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testLibraryUpgradeFailure(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return 1
})  
    """
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})
redis.register_function("test", "bar"); // this will fail
    '''
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('must be a function')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testLibraryUpgradeFailureWithStreamConsumer(env):
    """#!js name=foo
redis.register_stream_consumer("consumer", "stream", 1, false, async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
    """
    script = '''#!js name=foo
redis.register_stream_consumer("consumer", "stream", 1, false, async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
redis.register_function("test", "bar"); // this will fail
    '''
    env.cmd('XADD', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, '1', lambda: env.cmd('get', 'x'))
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('must be a function')
    env.cmd('XADD', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, '2', lambda: env.cmd('get', 'x'))

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testLibraryUpgradeFailureWithNotificationConsumer(env):
    """#!js name=foo
redis.register_notifications_consumer("consumer", "key", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
    """
    script = '''#!js name=foo
redis.register_notifications_consumer("consumer", "key", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
redis.register_function("test", "bar"); // this will fail
    '''
    env.cmd('set', 'key1', '1')
    runUntil(env, '1', lambda: env.cmd('get', 'x'))
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('must be a function')
    env.cmd('set', 'key1', '1')
    runUntil(env, '2', lambda: env.cmd('get', 'x'))

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testRedisCallNullReply(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return client.call('get', 'x');
})  
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal("undefined")

@gearsTest()
def testOOM(env):
    """#!js name=lib
redis.register_function("set", function(client, key, val){
    return client.call('set', key, val);
})  
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'set', 'x', '1').equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'set', 'x', '1').error().contains('OOM can not run the function when out of memory')

@gearsTest()
def testOOMOnAsyncFunction(env):
    """#!js name=lib
var continue_set = null;
var set_done = null;
var set_failed = null;

redis.register_function("async_set_continue",
    async function(client) {
        if (continue_set == null) {
            throw "no async set was triggered"
        }
        continue_set("continue");
        return await new Promise((resolve, reject) => {
            set_done = resolve;
            set_failed = reject
        })
    },
    ["allow-oom"]
)

redis.register_function("async_set_trigger", function(client, key, val){
    client.run_on_background(async function(client){
        await new Promise((resolve, reject) => {
            continue_set = resolve;
        })
        try {
            client.block(function(c){
                c.call('set', key, val);
            });
        } catch (error) {
            set_failed(error);
            return;
        }
        set_done("OK");
    });
    return "OK";
});
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_trigger', 'x', '1').equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_continue').error().contains('OOM Can not lock redis for write')

@gearsTest(envArgs={'useSlaves': True})
def testRunOnReplica(env):
    """#!js name=lib
redis.register_function("test1", function(client){
    return 1;
});

redis.register_function("test2", function(client){
    return 1;
},
['no-writes']);
    """
    replica = env.getSlaveConnection()
    env.expect('WAIT', '1', '7000').equal(1)

    try:
        replica.execute_command('RG.FUNCTION', 'CALL', 'lib', 'test1')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains('can not run a function that might perform writes on a replica', str(e))

    env.assertEqual(1, replica.execute_command('RG.FUNCTION', 'CALL', 'lib', 'test2'))

@gearsTest()
def testNoWritesFlag(env):
    """#!js name=lib
redis.register_function("my_set", function(client, key, val){
    return client.call('set', key, val);
},
['no-writes']);
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'my_set', 'foo', 'bar').error().contains('was called while write is not allowed')

@gearsTest()
def testBecomeReplicaWhenFunctionRunning(env):
    """#!js name=lib
var continue_set = null;
var set_done = null;
var set_failed = null;

redis.register_function("async_set_continue",
    async function(client) {
        if (continue_set == null) {
            throw "no async set was triggered"
        }
        continue_set("continue");
        return await new Promise((resolve, reject) => {
            set_done = resolve;
            set_failed = reject
        })
    },
    ["no-writes"]
)

redis.register_function("async_set_trigger", function(client, key, val){
    client.run_on_background(async function(client){
        await new Promise((resolve, reject) => {
            continue_set = resolve;
        })
        try {
            client.block(function(c){
                c.call('set', key, val);
            });
        } catch (error) {
            set_failed(error);
            return;
        }
        set_done("OK");
    });
    return "OK";
});
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_trigger', 'x', '1').equal('OK')
    env.expect('replicaof', '127.0.0.1', '33333')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_continue').error().contains('Can not lock redis for write on replica')
    env.expect('replicaof', 'no', 'one')

@gearsTest()
def testScriptTimeout(env):
    """#!js name=lib
redis.register_function("test1", function(client){
    while (true);
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testAsyncScriptTimeout(env):
    """#!js name=lib
redis.register_function("test1", async function(client){
    client.block(function(){
        while (true);
    });
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testTimeoutErrorNotCatchable(env):
    """#!js name=lib
redis.register_function("test1", async function(client){
    try {
        client.block(function(){
            while (true);
        });
    } catch (e) {
        return "catch timeout error"
    }
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testScriptLoadTimeout(env):
    script = """#!js name=lib
while(true);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testTimeoutOnStream(env):
    """#!js name=lib
redis.register_stream_consumer("consumer", "stream", 1, true, function(){
    while(true);
})
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['stream_consumers'][0]['streams'][0]['last_error'])

@gearsTest()
def testTimeoutOnStreamAsync(env):
    """#!js name=lib
redis.register_stream_consumer("consumer", "stream", 1, true, async function(c){
    c.block(function(){
        while(true);
    })
})
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['total_record_processed'])
    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['stream_consumers'][0]['streams'][0]['last_error'])

@gearsTest()
def testTimeoutOnNotificationConsumer(env):
    """#!js name=lib
redis.register_notifications_consumer("consumer", "", function(client, data) {
    while(true);
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('set', 'x', '1')
    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['notifications_consumers'][0]['last_error'])

@gearsTest()
def testTimeoutOnNotificationConsumerAsync(env):
    """#!js name=lib
redis.register_notifications_consumer("consumer", "", async function(client, data) {
    client.block(function(){
        while(true);
    })
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('set', 'x', '1')
    runUntil(env, 1, lambda: toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['notifications_consumers'][0]['num_failed'])
    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['notifications_consumers'][0]['last_error'])

@gearsTest()
def testOOM(env):
    """#!js name=lib
redis.register_function("test1", function(client){
    a = [1]
    while (true) {
        a = [a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a]
    }
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '1000000000').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')
