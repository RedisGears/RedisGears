from common import gearsTest
from common import TimeLimit
from common import toDictionary
from common import runUntil
from common import runFor
import time

@gearsTest()
def testBasicNotifications(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.registerFunction("n_notifications", function(){
    return n_notifications
})
    """

    env.expectTfcall('lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expectTfcall('lib', 'n_notifications').equal(1)
    env.expect('SET', 'X', '2').equal(True)
    env.expectTfcall('lib', 'n_notifications').equal(2)
    env.expect('SET', 'X', '1').equal(True)
    env.expectTfcall('lib', 'n_notifications').equal(3)

@gearsTest()
def testAsyncNotification(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", async function(client, data) {
    n_notifications += 1;
});

redis.registerAsyncFunction("n_notifications", async function(){
    return n_notifications
})
    """

    env.expectTfcallAsync('lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    runUntil(env, 1, lambda: env.tfcallAsync('lib', 'n_notifications'))
    env.expect('SET', 'X', '2').equal(True)
    runUntil(env, 2, lambda: env.tfcallAsync('lib', 'n_notifications'))
    env.expect('SET', 'X', '1').equal(True)
    runUntil(env, 3, lambda: env.tfcallAsync('lib', 'n_notifications'))

@gearsTest()
def testCallRedisOnNotification(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "key", async function(client, data) {
    client.block(function(client){
        client.call('incr', 'count')
    });
});
    """

    env.expect('GET', 'count').equal(None)
    env.expect('SET', 'key1', '1').equal(True)
    runUntil(env, '1', lambda: env.cmd('GET', 'count'))
    env.expect('SET', 'key2', '2').equal(True)
    runUntil(env, '2', lambda: env.cmd('GET', 'count'))
    env.expect('SET', 'key3', '1').equal(True)
    runUntil(env, '3', lambda: env.cmd('GET', 'count'))

@gearsTest()
def testNotificationsAreNotFiredFromWithinFunction(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.registerAsyncFunction("n_notifications", async function(){
    return n_notifications
});

redis.registerFunction("simple_set", function(client){
    return client.call('set', 'x', '1');
});
    """
    env.expectTfcallAsync('lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)
    env.expectTfcall('lib', 'simple_set').equal('OK')
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)

@gearsTest()
def testNotificationsAreNotFiredFromWithinFunction2(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    redis.log('inside key space trigger');
    client.call('xadd', 's', '*', 'foo', 'bar');
    client.call('set', 'x', '1'); // make sure this is not getting into infinit loop.
});

redis.registerStreamTrigger("stream", "", function(){
    return 1
});
    """
    env.expect('SET', 'X', '1').equal(True)

@gearsTest()
def testNotificationsAreNotFiredFromWithinAsyncFunction(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.registerAsyncFunction("n_notifications", async function(){
    return n_notifications
});

redis.registerAsyncFunction("simple_set", async function(client){
    return client.block(function(client){
        return client.call('set', 'x', '1');
    });
});
    """
    env.expectTfcallAsync('lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)
    env.expectTfcallAsync('lib', 'simple_set').equal('OK')
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)

@gearsTest()
def testNotificationsAreNotFiredFromWithinAnotherNotification(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    client.call('set', 'x' , '1');
    n_notifications += 1;
});

redis.registerAsyncFunction("n_notifications", async function(){
    return n_notifications
});
    """
    env.expectTfcallAsync('lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)

@gearsTest()
def testNotificationsAreNotFiredFromWithinStreamConsumer(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    redis.log(JSON.stringify(data));
    if (data.event == "set") {
        n_notifications += 1;
    }
});

redis.registerAsyncFunction("n_notifications", async function(){
    return n_notifications
});

redis.registerStreamTrigger("consumer", "stream",
    function(client) {
        client.call('set', 'X' , '2');
    },
    {
        isStreamTrimmed:true,
    }
);
    """
    env.expectTfcallAsync('lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)
    env.cmd('XADD', 'stream:1', '*', 'foo', 'bar')
    env.expect('GET', 'X').equal('2')
    env.expectTfcallAsync('lib', 'n_notifications').equal(1)

@gearsTest(decodeResponses=False)
def testNotificationsOnBinaryKey(env):
    """#!js api_version=1.0 name=lib
var n_notifications = 0;
var last_key = null;
var last_key_raw = null;
redis.registerKeySpaceTrigger("consumer", new Uint8Array([255]).buffer, function(client, data) {
    if (data.event == "set") {
        n_notifications += 1;
        last_data = data.key;
        last_key_raw = data.key_raw;
    }
});

redis.registerAsyncFunction("notifications_stats", async function(){
    return [
        n_notifications,
        last_key,
        last_key_raw
    ];
});
    """
    env.expectTfcallAsync('lib', 'notifications_stats').equal([0, None, None])
    env.expect('SET', b'\xff\xff', b'\xaa').equal(True)
    env.expectTfcallAsync('lib', 'notifications_stats').equal([1, None, b'\xff\xff'])

@gearsTest()
def testSyncNotificationsReturnPromise(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client) => {
    return client.executeAsync(async ()=>{return 1});
});
    """
    env.expect('SET', 'x', '1').equal(True)
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['num_success'])

@gearsTest()
def testSyncNotificationsReturnPromiseRaiseError(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client) => {
    return client.executeAsync(async ()=>{throw "SomeError"});
});
    """
    env.expect('SET', 'x', '1').equal(True)
    runUntil(env, 'SomeError', lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['last_error'])

@gearsTest()
def testAsyncNotificationsReturnPromise(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", async (client) => {
    return client.block((client) => {return client.executeAsync(async() => {return 1;})});
});
    """
    env.expect('SET', 'x', '1').equal(True)
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['num_success'])

@gearsTest()
def testAsyncNotificationsReturnPromiseRaiseError(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", async (client) => {
    return client.block((client) => {return client.executeAsync(async() => {throw "SomeError";})});
});
    """
    env.expect('SET', 'x', '1').equal(True)
    runUntil(env, 'SomeError', lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['last_error'])

@gearsTest()
def testSyncNotificationsReturnResolvedPromise(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client) => {
    var resolve_promise = null;
    var promise = new Promise((resolve, reject) => {
        resolve_promise = resolve;
    });
    resolve_promise(1)
    return promise;
});
    """
    env.expect('SET', 'x', '1').equal(True)
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['num_success'])

@gearsTest()
def testOnTriggerFiredCallback(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client, data) => {
    client.call('set', 'x', data.test);
},{
    onTriggerFired: (client, data) => {
        data.test = 'test';
    }
});
    """
    env.expect('SET', 'x', '1').equal(True)
    env.expect('GET', 'x').equal('test')

@gearsTest()
def testUnableToWriteOnTriggerFiredCallback(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client, data) => {
    client.call('set', 'x', data.test);
},{
    onTriggerFired: (client, data) => {
        client.call('set', 'x', 'test')
        data.test = 'test';
    }
});
    """
    env.expect('SET', 'x', '1').equal(True)
    env.expect('GET', 'x').equal('1')
    env.assertContains("Write command 'set' was called while write is not allowed", toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['last_error'])

@gearsTest()
def testReadOnTriggerFiredCallback(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client, data) => {
    client.call('set', 'x', (data.content + 1).toString());
},{
    onTriggerFired: (client, data) => {
        data.content = parseInt(client.call('get', 'x'), 10);
    }
});
    """
    env.expect('SET', 'x', '1').equal(True)
    env.expect('GET', 'x').equal('2')

@gearsTest()
def testTimeoutOnTriggerFiredCallback(env):
    """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client, data) => {
    client.call('set', 'x', data.test);
},{
    onTriggerFired: (client, data) => {
        while(true){}
    }
});
    """
    env.expect('SET', 'x', '1').equal(True)
    env.expect('GET', 'x').equal('1')
    env.assertContains("Execution was terminated due to OOM or timeout", toDictionary(env.cmd('TFUNCTION', 'LIST', 'v'))[0]['keyspace_triggers'][0]['last_error'])

@gearsTest()
def testWrongTypeForOnTriggerFiredCallbackArgument(env):
    script = """#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "", (client, data) => {
    client.call('set', 'x', data.test);
},{
    onTriggerFired: 1
});
    """
    env.expect('TFUNCTION', 'LOAD', script).error().contains("'onTriggerFired' argument to 'registerKeySpaceTrigger' must be a function")
