from common import gearsTest
from common import toDictionary
from common import runUntil
from redis import Redis

'''
todo:
1. tests for rdb save and load
'''

@gearsTest()
def testBasicJSInvocation(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", function(){
    return 1
})
    """
    env.expect('RG.FCALL', 'foo', 'test', 0).equal(1)

@gearsTest()
def testCommandInvocation(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", function(client){
    return client.call('ping')
})
    """
    env.expect('RG.FCALL', 'foo', 'test', 0).equal('PONG')

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgrade(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", function(client){
    return 1
})
    """
    script = '''#!js api_version=1.0 name=foo
redis.register_function("test", function(client){
    return 2
})
    '''
    env.expect('RG.FCALL', 'foo', 'test', 0).equal(1)
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).equal('OK')
    env.expect('RG.FCALL', 'foo', 'test', 0).equal(2)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgradeFailure(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", function(client){
    return 1
})
    """
    script = '''#!js api_version=1.0 name=foo
redis.register_function("test", function(client){
    return 2
})
redis.register_function("test", "bar"); // this will fail
    '''
    env.expect('RG.FCALL', 'foo', 'test', 0).equal(1)
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('must be a function')
    env.expect('RG.FCALL', 'foo', 'test', 0).equal(1)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgradeFailureWithStreamConsumer(env):
    """#!js api_version=1.0 name=foo
redis.register_stream_consumer("consumer", "stream", 1, false, async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
    """
    script = '''#!js api_version=1.0 name=foo
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
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgradeFailureWithNotificationConsumer(env):
    """#!js api_version=1.0 name=foo
redis.register_notifications_consumer("consumer", "key", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
    """
    script = '''#!js api_version=1.0 name=foo
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
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testRedisCallNullReply(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", function(client){
    return client.call('get', 'x');
})
    """
    env.expect('RG.FCALL', 'foo', 'test', 0).equal(None)

@gearsTest()
def testRedisOOM(env):
    """#!js api_version=1.0 name=lib
redis.register_function("set", function(client, key, val){
    return client.call('set', key, val);
})
    """
    env.expect('RG.FCALL', 'lib', 'set', '1', 'x', '1').equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expect('RG.FCALL', 'lib', 'set', '1', 'x', '1').error().contains('OOM can not run the function when out of memory')

@gearsTest()
def testRedisOOMOnAsyncFunction(env):
    """#!js api_version=1.0 name=lib
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
    env.expect('RG.FCALL', 'lib', 'async_set_trigger', '1', 'x', '1').equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expect('RG.FCALL', 'lib', 'async_set_continue', '0').error().contains('OOM Can not lock redis for write')

@gearsTest(withReplicas=True)
def testRunOnReplica(env):
    """#!js api_version=1.0 name=lib
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
        replica.execute_command('RG.FCALL', 'lib', 'test1', '0')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains('can not run a function that might perform writes on a replica', str(e))

    env.assertEqual(1, replica.execute_command('RG.FCALL', 'lib', 'test2', '0'))

@gearsTest(withReplicas=True)
def testFunctionDelReplicatedToReplica(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", function(client){
    return 1;
},
['no-writes']);
    """
    replica = env.getSlaveConnection()
    res = replica.execute_command('RG.FCALL', 'lib', 'test', '0')
    env.expect('RG.FUNCTION', 'DEL', 'lib').equal('OK')
    env.expect('WAIT', '1', '7000').equal(1)
    try:
        replica.execute_command('RG.FCALL', 'lib', 'test', '0')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains('Unknown library', str(e))


@gearsTest()
def testNoWritesFlag(env):
    """#!js api_version=1.0 name=lib
redis.register_function("my_set", function(client, key, val){
    return client.call('set', key, val);
},
['no-writes']);
    """
    env.expect('RG.FCALL', 'lib', 'my_set', '1', 'foo', 'bar').error().contains('was called while write is not allowed')

@gearsTest()
def testBecomeReplicaWhenFunctionRunning(env):
    """#!js api_version=1.0 name=lib
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
    env.expect('RG.FCALL', 'lib', 'async_set_trigger', '1', 'x', '1').equal('OK')
    env.expect('replicaof', '127.0.0.1', '33333')
    env.expect('RG.FCALL', 'lib', 'async_set_continue', '0').error().contains('Can not lock redis for write on replica')
    env.expect('replicaof', 'no', 'one')

@gearsTest()
def testScriptTimeout(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test1", function(client){
    while (true);
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FCALL', 'lib', 'test1', '0').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testAsyncScriptTimeout(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test1", async function(client){
    client.block(function(){
        while (true);
    });
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FCALL', 'lib', 'test1', '0').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testTimeoutErrorNotCatchable(env):
    """#!js api_version=1.0 name=lib
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
    env.expect('RG.FCALL', 'lib', 'test1', '0').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testScriptLoadTimeout(env):
    script = """#!js api_version=1.0 name=lib
while(true);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testTimeoutOnStream(env):
    """#!js api_version=1.0 name=lib
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
    """#!js api_version=1.0 name=lib
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
    """#!js api_version=1.0 name=lib
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
    """#!js api_version=1.0 name=lib
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

@gearsTest(v8MaxMemory=20 * 1024 * 1024)
def testV8OOM(env):
    code = """#!js api_version=1.0 name=lib

redis.register_function("test", function(client){
    return "OK";
});

redis.register_function("test1", function(client){
    a = []
    while (true) {
        a.push('foo')
    }
});

redis.register_notifications_consumer("consumer", "", function(client, data){
    return
});

redis.register_stream_consumer(
    "consumer", // consumer name
    "stream", // streams prefix
    1, // window
    false, // trim stream
    function(c, data) {
        return;
    }
);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '1000000000').equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', code).equal('OK')

    env.expect('RG.FCALL', 'lib', 'test1', '0').error().contains('Execution was terminated due to OOM or timeout')
    env.expect('RG.FCALL', 'lib', 'test', '0').error().contains('JS engine reached OOM state and can not run any more code')

    # make sure JS code is not running on key space notifications
    env.expect('set', 'x', '1').equal(True)
    env.assertEqual(toDictionary(env.cmd('RG.FUNCTION', 'list', 'library', 'lib', 'vv'))[0]['notifications_consumers'][0]['last_error'], 'JS engine reached OOM state and can not run any more code')

    # make sure JS code is not running to process stream data
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    env.assertEqual(toDictionary(env.cmd('RG.FUNCTION', 'list', 'library', 'lib', 'vv'))[0]['stream_consumers'][0]['streams'][0]['last_error'], 'JS engine reached OOM state and can not run any more code')

    # make sure we can not load any more libraries
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('JS engine reached OOM state and can not run any more code')

    # delete the library and make sure we can run JS code again
    env.expect('RG.FUNCTION', 'DEL', 'lib').equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', code).equal('OK')
    env.expect('RG.FCALL', 'lib', 'test', '0').equal('OK')

@gearsTest()
def testLibraryConfiguration(env):
    code = """#!js api_version=1.0 name=lib
redis.register_function("test1", function(){
    return redis.config;
});
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', '{"foo":"bar"}', code).equal("OK")
    env.expect('RG.FCALL', 'lib', 'test1', '0').equal(['foo', 'bar'])

@gearsTest()
def testLibraryConfigurationPersistAfterLoading(env):
    code = """#!js api_version=1.0 name=lib
redis.register_function("test1", function(){
    return redis.config;
});
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', '{"foo":"bar"}', code).equal("OK")
    env.expect('debug', 'reload').equal("OK")
    env.expect('RG.FCALL', 'lib', 'test1', '0').equal(['foo', 'bar'])

@gearsTest(enableGearsDebugCommands=True)
def testCallTypeParsing(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", function(client){
    var res;

    res = client.call("debug", "protocol", "string");
    if (typeof res !== "string") {
        throw `string protocol returned wrong type, typeof='${typeof res}'.`;
    }

    res = client.call("debug", "protocol", "integer");
    if (typeof res !== "bigint") {
        throw `integer protocol returned wrong type, typeof='${typeof res}'.`;
    }

    res = client.call("debug", "protocol", "double");
    if (typeof res !== "number") {
        throw `double protocol returned wrong type, typeof='${typeof res}'.`;
    }

    res = client.call("debug", "protocol", "bignum");
    if (typeof res !== "object" || res.__reply_type !== "big_number") {
        throw `bignum protocol returned wrong type, typeof='${typeof res}', __reply_type='${res.__reply_type}'.`;
    }

    res = client.call("debug", "protocol", "null");
    if (res !== null) {
        throw `null protocol returned wrong type, res='${res}'.`;
    }

    res = client.call("debug", "protocol", "array");
    if (!Array.isArray(res)) {
        throw `array protocol returned no array type.`;
    }

    res = client.call("debug", "protocol", "set");
    if (!(res instanceof Set)) {
        throw `set protocol returned no set type.`;
    }

    res = client.call("debug", "protocol", "map");
    if (typeof res !== "object") {
        throw `map protocol returned no map type.`;
    }

    res = client.call("debug", "protocol", "verbatim");
    if (typeof res !== "object" || res.__reply_type !== "verbatim" || res.__format !== "txt") {
        throw `verbatim protocol returned wrong type, typeof='${typeof res}', __reply_type='${res.__reply_type}', __format='${res.__format}'.`;
    }

    res = client.call("debug", "protocol", "true");
    if (typeof res !== "boolean" || !res) {
        throw `true protocol returned wrong type, typeof='${typeof res}', value='${res}'.`;
    }

    res = client.call("debug", "protocol", "false");
    if (typeof res !== "boolean" || res) {
        throw `true protocol returned wrong type, typeof='${typeof res}', value='${res}'.`;
    }

    return (()=>{var ret = new String("OK"); ret.__reply_type = "status"; return ret})();
});
    """
    env.expect('RG.FUNCTION', 'DEBUG', 'allow_unsafe_redis_commands').equal("OK")
    env.expect('RG.FCALL', 'lib', 'test', '0').equal("OK")

@gearsTest(enableGearsDebugCommands=True)
def testResp3Types(env):
    """#!js api_version=1.0 name=lib
redis.register_function("debug_protocol", function(client, arg){
    return client.call("debug", "protocol", arg);
});
    """
    env.expect('RG.FUNCTION', 'DEBUG', 'allow_unsafe_redis_commands').equal("OK")
    port = int(env.cmd('config', 'get', 'port')[1])

    # test resp3
    conn = Redis('localhost', port, protocol=3, decode_responses=True)
    
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'string'), 'Hello World')
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'integer'), 12345)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'double'), 3.141)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'bignum'), '1234567999999999999999999999999999999')
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'null'), None)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'array'), [0, 1, 2])
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'set'), set([0, 1, 2]))
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'map'), {1: True, 2: False, 0: False})
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'verbatim'), 'txt:This is a verbatim\nstring')
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'true'), True)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'false'), False)

    # test resp2
    conn = env.getConnection()

    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'string'), 'Hello World')
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'integer'), 12345)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'double'), "3.141")
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'bignum'), '1234567999999999999999999999999999999')
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'null'), None)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'array'), [0, 1, 2])
    env.assertEqual(sorted(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'set')), sorted([0, 1, 2]))
    env.assertEqual(sorted(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'map')), sorted([1, 1, 0, 0, 2, 0]))
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'verbatim'), 'This is a verbatim\nstring')
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'true'), True)
    env.assertEqual(conn.execute_command('RG.FCALL', 'lib', 'debug_protocol', '0', 'false'), False)

@gearsTest(enableGearsDebugCommands=True)
def testFunctionListResp3(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", function(){
    return "test";
});
    """
    port = int(env.cmd('config', 'get', 'port')[1])

    # test resp3
    conn = Redis('localhost', port, protocol=3, decode_responses=True)
    
    env.assertEqual(conn.execute_command('RG.FUNCTION', 'LIST'), [\
        {\
         'configuration': None,\
         'remote_functions': [],\
         'engine': 'js',\
         'name': 'lib',\
         'pending_jobs': 0,\
         'functions': ['test'],\
         'user': 'default',\
         'notifications_consumers': [],\
         'api_version': '1.0',\
         'stream_consumers': []\
        }\
    ])

@gearsTest()
def testNoAsyncFunctionOnMultiExec(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", async() => {return 'test'});
    """
    conn = env.getConnection()
    p = conn.pipeline()
    p.execute_command('RG.FCALL', 'lib', 'test', '0')
    try:
        p.execute()
        env.assertTrue(False, message='Except error on async function inside transaction')
    except Exception as e:
        env.assertContains('Blocking is not allow', str(e))

@gearsTest()
def testSyncFunctionWithPromiseOnMultiExec(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", () => {return new Promise((resume, reject) => {})});
    """
    conn = env.getConnection()
    p = conn.pipeline()
    p.execute_command('RG.FCALL', 'lib', 'test', '0')
    try:
        p.execute()
        env.assertTrue(False, message='Except error on async function inside transaction')
    except Exception as e:
        env.assertContains('Blocking is not allow', str(e))

@gearsTest()
def testAllowBlockAPI(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c) => {return c.allow_block()});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').equal(1)
    conn = env.getConnection()
    p = conn.pipeline()
    p.execute_command('RG.FCALL', 'lib', 'test', '0')
    res = p.execute()
    env.assertEqual(res, [0])

@gearsTest(decodeResponses=False)
def testRawArguments(env):
    """#!js api_version=1.0 name=lib
redis.register_function("my_set", (c, key, val) => {
    return c.call("set", key, val);
},
["raw-arguments"]);
    """
    env.expect('RG.FCALL', 'lib', 'my_set', '1', "x", "1").equal(b'OK')
    env.expect('get', 'x').equal(b'1')
    env.expect('RG.FCALL', 'lib', 'my_set', '1', b'\xaa', b'\xaa').equal(b'OK')
    env.expect('get', b'\xaa').equal(b'\xaa')

@gearsTest(decodeResponses=False)
def testRawArgumentsAsync(env):
    """#!js api_version=1.0 name=lib
redis.register_function("my_set", async (c, key, val) => {
    return c.block((c)=>{
        return c.call("set", key, val);
    });
},
["raw-arguments"]);
    """
    env.expect('RG.FCALL', 'lib', 'my_set', '1', "x", "1").equal(b'OK')
    env.expect('get', 'x').equal(b'1')
    env.expect('RG.FCALL', 'lib', 'my_set', '1', b'\xaa', b'\xaa').equal(b'OK')
    env.expect('get', b'\xaa').equal(b'\xaa')

@gearsTest(decodeResponses=False)
def testReplyWithBinaryData(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", () => {
    return new Uint8Array([255, 255, 255, 255]).buffer;
});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').equal(b'\xff\xff\xff\xff')

@gearsTest(decodeResponses=False)
def testRawCall(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c, key) => {
    return c.call_raw("get", key)
},
["raw-arguments"]);
    """
    env.expect('set', b'\xaa', b'\xaa').equal(True)
    env.expect('RG.FCALL', 'lib', 'test', '1', b'\xaa').equal(b'\xaa')

@gearsTest()
def testSimpleHgetall(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c, key) => {
    return c.call("hgetall", key)
},
["raw-arguments"]);
    """
    env.expect('hset', 'k', 'f', 'v').equal(True)
    env.expect('RG.FCALL', 'lib', 'test', '1', 'k').equal(['f', 'v'])


@gearsTest(decodeResponses=False)
def testBinaryFieldsNamesOnHashRaiseError(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c, key) => {
    return c.call("hgetall", key)
},
["raw-arguments"]);
    """
    env.expect('hset', b'\xaa', b'foo', b'\xaa').equal(True)
    env.expect('RG.FCALL', 'lib', 'test', '1', b'\xaa').error().contains('Could not decode value as string')

@gearsTest(decodeResponses=False)
def testBinaryFieldsNamesOnHash(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c, key) => {
    return typeof Object.keys(c.call_raw("hgetall", key))[0];
},
["raw-arguments"]);
    """
    env.expect('hset', b'\xaa', b'\xaa', b'\xaa').equal(True)
    env.expect('RG.FCALL', 'lib', 'test', '1', b'\xaa').error().contains('Binary map key is not supported')

@gearsTest()
def testFunctionListWithLibraryOption(env):
    code = """#!js api_version=1.0 name=lib
redis.register_function("test", (c, key) => {
    return typeof Object.keys(c.call_raw("hgetall", key))[0];
},
["raw-arguments", "allow-oom", "raw-arguments"]);

redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    num_events++;
})

redis.register_notifications_consumer("consumer", "", function(client, data) {});
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', '{"foo":"bar"}', code).equal("OK")
    env.assertEqual(toDictionary(env.cmd('RG.FUNCTION', 'list', 'library', 'lib', 'v'))[0]['engine'], 'js') # sanaty check
    env.assertEqual(toDictionary(env.cmd('RG.FUNCTION', 'list', 'library', 'lib', 'vv'))[0]['engine'], 'js') # sanaty check
    env.assertEqual(toDictionary(env.cmd('RG.FUNCTION', 'list', 'library', 'lib'), max_recursion=2)[0]['engine'], 'js') # sanaty check
    env.assertEqual(toDictionary(env.cmd('RG.FUNCTION', 'list', 'withcode'), max_recursion=2)[0]['engine'], 'js') # sanaty check

@gearsTest()
def testReplyWithSimpleString(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", async () => {
    var res = new String('test');
    res.__reply_type = 'status';
    return res;
});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').equal("test")

@gearsTest()
def testReplyWithDouble(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", () => {
    return 1.1;
});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').contains("1.1")

@gearsTest()
def testReplyWithDoubleAsync(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", async () => {
    return 1.1;
});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').contains("1.1")

@gearsTest()
def testRunOnBackgroundThatRaisesError(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c) => {
    return c.run_on_background(async (c) => {
        throw "Some Error"
    });
});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').error().equal("Some Error")

@gearsTest()
def testRunOnBackgroundThatReturnInteger(env):
    """#!js api_version=1.0 name=lib
redis.register_function("test", (c) => {
    return c.run_on_background(async (c) => {
        return 1;
    });
});
    """
    env.expect('RG.FCALL', 'lib', 'test', '0').equal(1)

@gearsTest()
def testOver100Isolates(env):
    code = """#!js api_version=1.0 name=lib%d
redis.register_function("test", (c) => {
    return c.run_on_background(async (c) => {
        return 1;
    });
});
    """
    for i in range(101):
        env.expect('RG.FUNCTION', 'LOAD', code % (i)).equal('OK')

