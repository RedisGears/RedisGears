from common import gearsTest
from common import toDictionary
from common import runUntil
from redis import Redis
import time

MODULE_NAME = "redisgears_2"

'''
todo:
1. tests for rdb save and load
'''

@gearsTest()
def testBasicJSInvocation(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", function(){
    return 1
})
    """
    env.expectTfcall('foo', 'test').equal(1)

@gearsTest()
def testCommandInvocation(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return client.call('ping')
})
    """
    env.expectTfcall('foo', 'test').equal('PONG')

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgrade(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 1
})
    """
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 2
})
    '''
    env.expectTfcall('foo', 'test').equal(1)
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).equal('OK')
    env.expectTfcall('foo', 'test').equal(2)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('TFUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgradeFailure(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 1
})
    """
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 2
})
redis.registerFunction("test", "bar"); // this will fail
    '''
    env.expectTfcall('foo', 'test').equal(1)
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains('must be a function')
    env.expectTfcall('foo', 'test').equal(1)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('TFUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgradeFailureWithStreamConsumer(env):
    """#!js api_version=1.0 name=foo
redis.registerStreamTrigger("consumer", "stream", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
    """
    script = '''#!js api_version=1.0 name=foo
redis.registerStreamTrigger("consumer", "stream", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
redis.registerFunction("test", "bar"); // this will fail
    '''
    env.cmd('XADD', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, '1', lambda: env.cmd('get', 'x'))
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains('must be a function')
    env.cmd('XADD', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, '2', lambda: env.cmd('get', 'x'))

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('TFUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest(enableGearsDebugCommands=True)
def testLibraryUpgradeFailureWithNotificationConsumer(env):
    """#!js api_version=1.0 name=foo
redis.registerKeySpaceTrigger("consumer", "key", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
    """
    script = '''#!js api_version=1.0 name=foo
redis.registerKeySpaceTrigger("consumer", "key", async function(c){
    c.block(function(c) {
        c.call('incr', 'x')
    })
})
redis.registerFunction("test", "bar"); // this will fail
    '''
    env.cmd('set', 'key1', '1')
    runUntil(env, '1', lambda: env.cmd('get', 'x'))
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains('must be a function')
    env.cmd('set', 'key1', '1')
    runUntil(env, '2', lambda: env.cmd('get', 'x'))

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('TFUNCTION', 'DEBUG', 'js', 'isolates_aggregated_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testRedisCallNullReply(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return client.call('get', 'x');
})
    """
    env.expectTfcall('foo', 'test').equal(None)

@gearsTest()
def testRedisOOM(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("set", function(client, key, val){
    return client.call('set', key, val);
})
    """
    env.expectTfcall('lib', 'set', ['x'], ['1']).equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expectTfcall('lib', 'set', ['x'], ['1']).error().contains('OOM can not run the function when out of memory')

@gearsTest()
def testRedisOOMOnAsyncFunction(env):
    """#!js api_version=1.0 name=lib
var continue_set = null;
var set_done = null;
var set_failed = null;

redis.registerAsyncFunction("async_set_continue",
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
    {
        flags: [redis.functionFlags.ALLOW_OOM]
    }
)

redis.registerFunction("async_set_trigger", function(client, key, val){
    client.executeAsync(async function(client){
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
    env.expectTfcall('lib', 'async_set_trigger', ['x'], ['1']).equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expectTfcallAsync('lib', 'async_set_continue').error().contains('OOM Can not lock redis for write')

@gearsTest(withReplicas=True)
def testRunOnReplica(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test1",
    function(client){
        return 1;
    });

    redis.registerFunction("test2", function(client){
        return 1;
    },
    {
        flags: [redis.functionFlags.NO_WRITES],
    }
);
    """
    replica = env.getSlaveConnection()
    env.expect('WAIT', '1', '7000').equal(1)

    try:
        env.tfcall('lib', 'test1', c=replica)
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains('can not run a function that might perform writes on a replica', str(e))

    env.assertEqual(1, env.tfcall('lib', 'test2', c=replica))

@gearsTest(withReplicas=True)
def testFunctionDelReplicatedToReplica(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    function(client){
        return 1;
    },
    {
        flags: [redis.functionFlags.NO_WRITES]
    }
);
    """
    replica = env.getSlaveConnection()
    res = env.tfcall('lib', 'test', c=replica)
    env.expect('TFUNCTION', 'DELETE', 'lib').equal('OK')
    env.expect('WAIT', '1', '7000').equal(1)
    try:
        env.tfcall('lib', 'test', c=replica)
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains('Unknown library', str(e))


@gearsTest()
def testNoWritesFlag(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("my_set",
    function(client, key, val){
        return client.call('set', key, val);
    },
    {
        flags: [redis.functionFlags.NO_WRITES]
    }
);
    """
    env.expectTfcall('lib', 'my_set', ['foo'], ['bar']).error().contains('was called while write is not allowed')

@gearsTest()
def testBecomeReplicaWhenFunctionRunning(env):
    """#!js api_version=1.0 name=lib
var continue_set = null;
var set_done = null;
var set_failed = null;

redis.registerAsyncFunction("async_set_continue",
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
    {
        flags: [redis.functionFlags.NO_WRITES]
    }
)

redis.registerFunction("async_set_trigger", function(client, key, val){
    client.executeAsync(async function(client){
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
    env.expectTfcall('lib', 'async_set_trigger', ['x'], ['1']).equal('OK')
    env.expect('replicaof', '127.0.0.1', '33333')
    env.expectTfcallAsync('lib', 'async_set_continue').error().contains('Can not lock redis for write on replica')
    env.expect('replicaof', 'no', 'one')

@gearsTest()
def testScriptTimeout(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test1", function(client){
    while (true);
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expectTfcall('lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testAsyncScriptTimeout(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test1", async function(client){
    client.block(function(){
        while (true);
    });
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expectTfcallAsync('lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest(enableGearsDebugCommands=True, gearsConfig={"v8-flags": "'--expose_gc'"})
def testAsyncScriptTimeout2(env):
    script = """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test1", async function(client){
    await client.block(function(c){
        return c.callAsync('blpop', 'l', '0');
    });
    client.block(function(){
        while (true);
    });
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('tfunction', 'debug', 'js', 'avoid_global_allow_list').equal('OK')
    env.expect('tfunction', 'LOAD', script).equal('OK')
    future = env.noBlockingTfcallAsync('lib', 'test1')
    def run_v8_gc():
        env.cmd('tfunction', 'debug', 'js', 'request_v8_gc_for_debugging')
        res = env.cmd('info', 'clients')['blocked_clients']
        return res
    runUntil(env, 1, run_v8_gc)
    env.expect('RPUSH', 'l', '1').equal(1)
    runUntil(env, 0, run_v8_gc)
    future.expectError('Promise was dropped without been resolved')



@gearsTest()
def testExecuteAsyncScriptTimeout(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test1", function(client){
    return client.executeAsync(async (c) =>{
        c.block(function(){
            while (true);
        });
    });

});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expectTfcallAsync('lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testTimeoutErrorNotCatchable(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test1", async function(client){
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
    env.expectTfcallAsync('lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testScriptLoadTimeout(env):
    script = """#!js api_version=1.0 name=lib
while(true);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.expect('TFUNCTION', 'LOAD', script).error().contains('Execution was terminated due to OOM or timeout')

@gearsTest()
def testTimeoutOnStream(env):
    """#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "stream",
    function(){
        while(true);
    },
    {
        isStreamTrimmed: true
    }
);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['stream_triggers'][0]['streams'][0]['last_error'])

@gearsTest()
def testTimeoutOnStreamAsync(env):
    """#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "stream",
    async function(c){
        c.block(function(){
            while(true);
        })
    },
    {
        isStreamTrimmed: true,
    }
);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['stream_triggers'][0]['streams'][0]['last_error'])

@gearsTest()
def testTimeoutOnNotificationConsumer(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    while(true);
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('set', 'x', '1')
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['keyspace_triggers'][0]['last_error'])

@gearsTest()
def testTimeoutOnNotificationConsumerAsync(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", async function(client, data) {
    client.block(function(){
        while(true);
    })
});
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '100').equal('OK')
    env.cmd('set', 'x', '1')
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['keyspace_triggers'][0]['num_failed'])
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Execution was terminated due to OOM or timeout', res[0]['keyspace_triggers'][0]['last_error'])

@gearsTest(v8MaxMemory=20 * 1024 * 1024)
def testV8OOM(env):
    code = """#!js api_version=1.0 name=lib

redis.registerFunction("test", function(client){
    return "OK";
});

redis.registerFunction("test1", function(client){
    a = []
    while (true) {
        a.push('foo')
    }
});

redis.registerKeySpaceTrigger("consumer", "", function(client, data){
    return
});

redis.registerStreamTrigger(
    "consumer", // consumer name
    "stream", // streams prefix
    function(c, data) {
        return;
    }
);
    """
    env.expect('config', 'set', 'redisgears_2.lock-redis-timeout', '1000000000').equal('OK')
    env.expect('TFUNCTION', 'LOAD', code).equal('OK')

    env.expectTfcall('lib', 'test1').error().contains('Execution was terminated due to OOM or timeout')
    env.expectTfcall('lib', 'test').error().contains('JS engine reached OOM state and can not run any more code')

    # make sure JS code is not running on key space notifications
    env.expect('set', 'x', '1').equal(True)
    env.assertEqual(toDictionary(env.cmd('TFUNCTION', 'list', 'library', 'lib', 'vv'))[0]['keyspace_triggers'][0]['last_error'], 'JS engine reached OOM state and can not run any more code')

    # make sure JS code is not running to process stream data
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    env.assertEqual(toDictionary(env.cmd('TFUNCTION', 'list', 'library', 'lib', 'vv'))[0]['stream_triggers'][0]['streams'][0]['last_error'], 'JS engine reached OOM state and can not run any more code')

    # make sure we can not load any more libraries
    env.expect('TFUNCTION', 'LOAD', code).error().contains('JS engine reached OOM state and can not run any more code')

    # delete the library and make sure we can run JS code again
    env.expect('TFUNCTION', 'DELETE', 'lib').equal('OK')
    env.expect('TFUNCTION', 'LOAD', code).equal('OK')
    env.expectTfcall('lib', 'test').equal('OK')

@gearsTest(v8MaxMemory=20 * 1024 * 1024)
def testV8OOMOnFunctionLoad(env):
    code = """#!js api_version=1.0 name=lib%d
redis.registerFunction("test", function(client){
    return "OK";
});
    """
    try:
        for i in range(100):
            env.cmd('TFUNCTION', 'LOAD', code % i)
        env.assertTrue(False, message='Except OOM on function load')
    except Exception as e:
        env.assertIn('JS engine reached OOM state', str(e))

@gearsTest()
def testLibraryConfiguration(env):
    code = """#!js api_version=1.0 name=lib
redis.registerFunction("test1", function(){
    return redis.config;
});
    """
    env.expect('TFUNCTION', 'LOAD', 'CONFIG', '{"foo":"bar"}', code).equal("OK")
    env.expectTfcall('lib', 'test1').equal(['foo', 'bar'])

@gearsTest()
def testLibraryConfigurationPersistAfterLoading(env):
    code = """#!js api_version=1.0 name=lib
redis.registerFunction("test1", function(){
    return redis.config;
});
    """
    env.expect('TFUNCTION', 'LOAD', 'CONFIG', '{"foo":"bar"}', code).equal("OK")
    env.expect('debug', 'reload').equal("OK")
    env.expectTfcall('lib', 'test1').equal(['foo', 'bar'])

@gearsTest()
def testLibraryLoadingTimesout(env):
    code = """#!js api_version=1.0 name=lib
redis.registerFunction("test1", function(){
    return redis.config;
});
while (true) {
    // do nothing
}
    """
    TIMEOUT_EXPECTED_MS = 2000

    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.lock-redis-timeout', TIMEOUT_EXPECTED_MS).equal("OK")
    loading_start_time = time.time()
    env.expect('TFUNCTION', 'LOAD', code).equal("Failed loading library: Err Execution was terminated due to OOM or timeout.")
    loading_end_time = time.time()
    env.assertTrue(loading_end_time - loading_start_time >= TIMEOUT_EXPECTED_MS / 1000, message="The timeout hasn't happened correctly.")
    env.expectTfcall('lib', 'test1').equal("Unknown library lib")

@gearsTest()
def testLibraryLoadingFromRdbTimesout(env):
    code = """#!js api_version=1.0 name=lib
redis.registerFunction("test1", function(){
    return "GOODJOB";
});

const d = new Date();
let time = d.getTime();

while (true) {
    if (new Date().getTime() - time > 1000) {
        break;
    }
}
    """
    TIMEOUT_EXPECTED_MS = 1200
    MINIMAL_TIMEOUT_MS = 100

    # Initially it fails due to the default lock-redis-timeout of 500ms.
    env.expect('TFUNCTION', 'LOAD', code).equal("Failed loading library: Err Execution was terminated due to OOM or timeout.")

    # We increase the timeout to the expected value + some value to
    # allow for some slack.
    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.lock-redis-timeout', TIMEOUT_EXPECTED_MS).equal("OK")
    env.expect('TFUNCTION', 'LOAD', code).equal("OK")
    env.expectTfcall('lib', 'test1').equal("GOODJOB")

    # Now dump to RDB and load from there. This case makes sure the
    # db-loading-lock-redis-timeout is used, by setting it to a minimal
    # possible value.
    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.lock-redis-timeout', MINIMAL_TIMEOUT_MS).equal("OK")
    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.db-loading-lock-redis-timeout', MINIMAL_TIMEOUT_MS).equal("OK")
    env.expect('debug', 'reload').equal("Error trying to load the RDB dump, check server logs.")

@gearsTest(withReplicas=True)
def testLoadingFunctionOnReplicaTakesLoadingTimeout(env):
    code = """#!js api_version=1.0 name=lib
redis.registerFunction("test1", function(){
    return "GOODJOB";
},{flags: [redis.functionFlags.NO_WRITES]});

const d = new Date();
let time = d.getTime();

while (true) {
    if (new Date().getTime() - time > 1000) {
        break;
    }
}
    """
    TIMEOUT_EXPECTED_MS = 1200

    # Initially it fails due to the default lock-redis-timeout of 500ms.
    env.expect('TFUNCTION', 'LOAD', code).equal("Failed loading library: Err Execution was terminated due to OOM or timeout.")

    # We increase the timeout to the expected value + some value to
    # allow for some slack.
    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.lock-redis-timeout', TIMEOUT_EXPECTED_MS).equal("OK")
    env.expect('TFUNCTION', 'LOAD', code).equal("OK")
    env.expectTfcall('lib', 'test1').equal("GOODJOB")

    # Waiting for replica to get the function load command
    env.expect('wait', '1', '5000').equal(1)

    # Making sure replica loaded the library successfully
    replica = env.getSlaveConnection()
    env.assertEqual(env.tfcall('lib', 'test1', c=replica), "GOODJOB")

@gearsTest()
def testRdbTimeoutCantBeLessThanLoadTimeout(env):
    MINIMAL_TIMEOUT_MS = 100

    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.db-loading-lock-redis-timeout', MINIMAL_TIMEOUT_MS).contains("value can't be less than lock-redis-timeout")
    # 500 is the default for the lock-redis-timeout.
    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.db-loading-lock-redis-timeout', 501).equal("OK")
    # Test that changing the lock-redis-timeout also increases the
    # rdb one, if it would be lower.
    env.expect('CONFIG', 'SET', f'{MODULE_NAME}.lock-redis-timeout', 502).equal("OK")
    env.expect('CONFIG', 'GET', f'{MODULE_NAME}.db-loading-lock-redis-timeout').apply(lambda v: int(v[1])).equal(502)

@gearsTest(enableGearsDebugCommands=True)
def testCallTypeParsing(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", function(client){
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
    env.expect('TFUNCTION', 'DEBUG', 'allow_unsafe_redis_commands').equal("OK")
    env.expectTfcall('lib', 'test').equal("OK")

@gearsTest(enableGearsDebugCommands=True)
def testResp3Types(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("debug_protocol", function(client, arg){
    return client.call("debug", "protocol", arg);
});
    """
    env.expect('TFUNCTION', 'DEBUG', 'allow_unsafe_redis_commands').equal("OK")
    conn = env.getResp3Connection()

    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['string'], c=conn), 'Hello World')
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['integer'], c=conn), 12345)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['double'], c=conn), 3.141)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['bignum'], c=conn), '1234567999999999999999999999999999999')
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['null'], c=conn), None)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['array'], c=conn), [0, 1, 2])
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['set'], c=conn), set([0, 1, 2]))
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['map'], c=conn), {1: True, 2: False, 0: False})
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['verbatim'], c=conn), 'txt:This is a verbatim\nstring')
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['true'], c=conn), True)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['false'], c=conn), False)

    # test resp2
    conn = env.getConnection()

    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['string'], c=conn), 'Hello World')
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['integer'], c=conn), 12345)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['double'], c=conn), "3.141")
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['bignum'], c=conn), '1234567999999999999999999999999999999')
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['null'], c=conn), None)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['array'], c=conn), [0, 1, 2])
    env.assertEqual(sorted(env.tfcall('lib', 'debug_protocol', [], ['set'], c=conn)), sorted([0, 1, 2]))
    env.assertEqual(sorted(env.tfcall('lib', 'debug_protocol', [], ['map'], c=conn)), sorted([1, 1, 0, 0, 2, 0]))
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['verbatim'], c=conn), 'This is a verbatim\nstring')
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['true'], c=conn), True)
    env.assertEqual(env.tfcall('lib', 'debug_protocol', [], ['false'], c=conn), False)

@gearsTest(enableGearsDebugCommands=True)
def testFunctionListResp3(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", function(){
    return "test";
});
    """
    conn = env.getResp3Connection()

    env.assertEqual(conn.execute_command('TFUNCTION', 'LIST'), [\
        {\
         'configuration': None,\
         'cluster_functions': [],\
         'engine': 'js',\
         'name': 'lib',\
         'pending_jobs': 0,\
         'functions': ['test'],\
         'user': 'default',\
         'keyspace_triggers': [],\
         'api_version': '1.0',\
         'stream_triggers': [],\
         'pending_async_calls': []\
        }\
    ])

@gearsTest()
def testNoAsyncFunctionOnMultiExec(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test", async() => {return 'test'});
    """
    conn = env.getConnection()
    p = conn.pipeline()
    env.tfcallAsync('lib', 'test', c=p)
    try:
        p.execute()
        env.assertTrue(False, message='Except error on async function inside transaction')
    except Exception as e:
        env.assertContains('Blocking is not allow', str(e))

@gearsTest()
def testSyncFunctionWithPromiseOnMultiExec(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", () => {return new Promise((resume, reject) => {})});
    """
    conn = env.getConnection()
    p = conn.pipeline()
    env.tfcall('lib', 'test', c=p)
    try:
        p.execute()
        env.assertTrue(False, message='Except error on async function inside transaction')
    except Exception as e:
        env.assertContains('Blocking is not allow', str(e))

@gearsTest()
def testAllowBlockAPI(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", (c) => {return c.isBlockAllowed()});
    """
    env.expectTfcall('lib', 'test').equal(0)
    env.expectTfcallAsync('lib', 'test').equal(1)
    conn = env.getConnection()
    p = conn.pipeline()
    env.tfcall('lib', 'test', c=p)
    env.tfcallAsync('lib', 'test', c=p)
    res = p.execute()
    env.assertEqual(res, [0, 0])

@gearsTest(decodeResponses=False)
def testRawArguments(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("my_set",
    (c, key, val) => {
        return c.call("set", key, val);
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
    """
    env.expectTfcall('lib', 'my_set', ["x"], ["1"]).equal(b'OK')
    env.expect('get', 'x').equal(b'1')
    env.expectTfcall('lib', 'my_set', [b'\xaa'], [b'\xaa']).equal(b'OK')
    env.expect('get', b'\xaa').equal(b'\xaa')

@gearsTest(decodeResponses=False)
def testRawArgumentsAsync(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("my_set",
    async (c, key, val) => {
        return c.block((c)=>{
            return c.call("set", key, val);
        });
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
    """
    env.expectTfcallAsync('lib', 'my_set', ["x"], ["1"]).equal(b'OK')
    env.expect('get', 'x').equal(b'1')
    env.expectTfcallAsync('lib', 'my_set', [b'\xaa'], [b'\xaa']).equal(b'OK')
    env.expect('get', b'\xaa').equal(b'\xaa')

@gearsTest(decodeResponses=False)
def testReplyWithBinaryData(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", () => {
    return new Uint8Array([255, 255, 255, 255]).buffer;
});
    """
    env.expectTfcall('lib', 'test').equal(b'\xff\xff\xff\xff')

@gearsTest(decodeResponses=False)
def testRawCall(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    (c, key) => {
        return c.callRaw("get", key)
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
    """
    env.expect('set', b'\xaa', b'\xaa').equal(True)
    env.expectTfcall('lib', 'test', [b'\xaa']).equal(b'\xaa')

@gearsTest()
def testSimpleHgetall(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    (c, key) => {
        return c.call("hgetall", key)
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
    """
    env.expect('hset', 'k', 'f', 'v').equal(True)
    env.expectTfcall('lib', 'test', ['k']).equal(['f', 'v'])


@gearsTest(decodeResponses=False)
def testBinaryFieldsNamesOnHashRaiseError(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    (c, key) => {
        return c.call("hgetall", key)
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
    """
    env.expect('hset', b'\xaa', b'foo', b'\xaa').equal(True)
    env.expectTfcall('lib', 'test', [b'\xaa']).error().contains('Could not decode value as string')

@gearsTest(decodeResponses=False)
def testBinaryFieldsNamesOnHash(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    (c, key) => {
        return typeof Object.keys(c.callRaw("hgetall", key))[0];
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
    """
    env.expect('hset', b'\xaa', b'\xaa', b'\xaa').equal(True)
    env.expectTfcall('lib', 'test', [b'\xaa']).error().contains('Binary map key is not supported')

@gearsTest()
def testFunctionListWithLibraryOption(env):
    code = """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    (c, key) => {
        return typeof Object.keys(c.callRaw("hgetall", key))[0];
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS, redis.functionFlags.ALLOW_OOM]
    }
);

redis.registerStreamTrigger("consumer", "stream", function(){
    num_events++;
})

redis.registerKeySpaceTrigger("consumer", "", function(client, data) {});
    """
    env.expect('TFUNCTION', 'LOAD', 'CONFIG', '{"foo":"bar"}', code).equal("OK")
    env.assertEqual(toDictionary(env.cmd('TFUNCTION', 'list', 'library', 'lib', 'v'))[0]['engine'], 'js') # sanaty check
    env.assertEqual(toDictionary(env.cmd('TFUNCTION', 'list', 'library', 'lib', 'vv'))[0]['engine'], 'js') # sanaty check
    env.assertEqual(toDictionary(env.cmd('TFUNCTION', 'list', 'library', 'lib'), max_recursion=2)[0]['engine'], 'js') # sanaty check
    env.assertEqual(toDictionary(env.cmd('TFUNCTION', 'list', 'withcode'), max_recursion=2)[0]['engine'], 'js') # sanaty check

@gearsTest()
def testReplyWithSimpleString(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test", async () => {
    var res = new String('test');
    res.__reply_type = 'status';
    return res;
});
    """
    env.expectTfcallAsync('lib', 'test').equal("test")

@gearsTest()
def testReplyWithDouble(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", () => {
    return 1.1;
});
    """
    env.expectTfcall('lib', 'test').contains("1.1")

@gearsTest()
def testReplyWithDoubleAsync(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction("test", async () => {
    return 1.1;
});
    """
    env.expectTfcallAsync('lib', 'test').contains("1.1")

@gearsTest()
def testRunOnBackgroundThatRaisesError(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", (c) => {
    return c.executeAsync(async (c) => {
        throw "Some Error"
    });
});
    """
    env.expectTfcallAsync('lib', 'test').error().equal("Some Error")

@gearsTest()
def testRunOnBackgroundThatReturnInteger(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test", (c) => {
    return c.executeAsync(async (c) => {
        return 1;
    });
});
    """
    env.expectTfcallAsync('lib', 'test').equal(1)

@gearsTest()
def testOver100Isolates(env):
    code = """#!js api_version=1.0 name=lib%d
redis.registerFunction("test", (c) => {
    return c.executeAsync(async (c) => {
        return 1;
    });
});
    """
    for i in range(101):
        env.expect('TFUNCTION', 'LOAD', code % (i)).equal('OK')

@gearsTest(useAof=True)
def testNoNotificationOnAOFLoading(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client) => {
    client.call('incr', 'notification');
});
    """
    env.expect('SET', 'x', '1').equal(True)
    env.expect('GET', 'notification').equal('1')
    env.expect('DEBUG', 'LOADAOF').equal('OK')
    # make sure notification was not fired.
    env.expect('GET', 'notification').equal('1')
    env.expect('SET', 'x', '2').equal(True)
    env.expect('GET', 'notification').equal('2')

@gearsTest(useAof=True)
def testFunctionDescription(env):
    """#!js api_version=1.0 name=lib
redis.registerFunction("test",
    () => {
        return "foo";
    },
    {
        description: "Some function",
    }
);
    """
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Some function', res[0]['functions'][0]['description'])

@gearsTest(useAof=True)
def testTriggerDescription(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("test", "",
    () => {
        return "foo";
    },
    {
        description: "Some function",
    }
);
    """
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Some function', res[0]['keyspace_triggers'][0]['description'])

@gearsTest(useAof=True)
def testStreamTriggerDescription(env):
    """#!js api_version=1.0 name=lib
redis.registerStreamTrigger("test", "",
    () => {
        return "foo";
    },
    {
        description: "Some function",
    }
);
    """
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 6)
    env.assertContains('Some function', res[0]['stream_triggers'][0]['description'])

@gearsTest()
def testNoNotificationsOnSlave(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("test", "",
    () => {
        n_notifications += 1;
    }
);
redis.registerFunction("n_notifications", ()=>{return n_notifications;}, {flags:[redis.functionFlags.NO_WRITES]});
    """
    env.cmd('get', 'x') # should trigger key miss notification
    env.expectTfcall('lib', 'n_notifications').equal(1)
    env.expect('replicaof', 'localhost', '1111').equal('OK')
    env.cmd('get', 'x') # should trigger key miss notification but we should not count it because we are a replica
    env.expectTfcall('lib', 'n_notifications').equal(1)

@gearsTest()
def testAvoidReplicationTrafficOnAsyncFunction(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerAsyncFunction("test",
    async (client) => {
        while (true) {
            client.block((c)=>{
                c.call('incr', 'x');
            });
        }
    }
);
    """
    future = env.noBlockingTfcallAsync('lib', 'test')
    runUntil(env, True, lambda: int(env.cmd('get', 'x')) > 2)
    env.expect('CLIENT', 'PAUSE', '1000', 'all').equal('OK')
    future.expectError('Can not lock redis for write')

@gearsTest()
def testAvoidReplicationTrafficOnAsyncFunction(env):
    """#!js api_version=1.0 name=lib
var continue_run = null;

redis.registerFunction("continue",
    function() {
        if (continue_run == null) {
            return "no async set was triggered";
        }
        continue_run("OK");
        return "OK";
    },
    {
        flags: [redis.functionFlags.ALLOW_OOM]
    }
)

redis.registerFunction("start", function(client, key, val){
    return new Promise((resolve, reject) => {
        continue_run = resolve;
    });
});
    """
    future = env.noBlockingTfcallAsync('lib', 'start')
    runUntil(env, 'OK', lambda: env.tfcall('lib', 'continue'))
    env.expectTfcall('lib', 'continue').equal('OK')
    future.equal('OK')
