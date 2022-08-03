from common import gearsTest
from common import TimeLimit
from common import toDictionary
from common import runUntil
from common import runFor
import time

@gearsTest()
def testBasicNotifcations(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.register_function("n_notifications", function(){
    return n_notifications
})
    """

    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)
    env.expect('SET', 'X', '2').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(2)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(3)

@gearsTest()
def testAsyncNotification(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", async function(client, data) {
    n_notifications += 1;
});

redis.register_function("n_notifications", async function(){
    return n_notifications
})
    """

    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    runUntil(env, 1, lambda: env.cmd('RG.FCALL', 'lib', 'n_notifications', '0'))
    env.expect('SET', 'X', '2').equal(True)
    runUntil(env, 2, lambda: env.cmd('RG.FCALL', 'lib', 'n_notifications', '0'))
    env.expect('SET', 'X', '1').equal(True)
    runUntil(env, 3, lambda: env.cmd('RG.FCALL', 'lib', 'n_notifications', '0'))

@gearsTest()
def testCallRedisOnNotification(env):
    """#!js name=lib
redis.register_notifications_consumer("consumer", "key", async function(client, data) {
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
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.register_function("n_notifications", async function(){
    return n_notifications
});

redis.register_function("simple_set", function(client){
    return client.call('set', 'x', '1');
});
    """
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)
    env.expect('RG.FCALL', 'lib', 'simple_set', '0').equal('OK')
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)

@gearsTest()
def testNotificationsAreNotFiredFromWithinAsyncFunction(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.register_function("n_notifications", async function(){
    return n_notifications
});

redis.register_function("simple_set", async function(client){
    return client.block(function(client){
        return client.call('set', 'x', '1');
    });
});
    """
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)
    env.expect('RG.FCALL', 'lib', 'simple_set', '0').equal('OK')
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)

@gearsTest()
def testNotificationsAreNotFiredFromWithinAnotherNotification(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", function(client, data) {
    client.call('set', 'x' , '1');
    n_notifications += 1;
});

redis.register_function("n_notifications", async function(){
    return n_notifications
});
    """
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)

@gearsTest()
def testNotificationsAreNotFiredFromWithinStreamConsumer(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", function(client, data) {
    redis.log(JSON.stringify(data));
    if (data.event == "set") {
        n_notifications += 1;
    }
});

redis.register_function("n_notifications", async function(){
    return n_notifications
});

redis.register_stream_consumer("consumer", "stream", 1, true, function(client) {
    client.call('set', 'X' , '2');
}) 
    """
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)
    env.cmd('XADD', 'stream:1', '*', 'foo', 'bar')
    env.expect('GET', 'X').equal('2')
    env.expect('RG.FCALL', 'lib', 'n_notifications', '0').equal(1)

@gearsTest(decodeResponses=False)
def testNotificationsOnBinaryKey(env):
    """#!js name=lib
var n_notifications = 0;
var last_key = null;
var last_key_raw = null;
redis.register_notifications_consumer("consumer", new Uint8Array([255]).buffer, function(client, data) {
    if (data.event == "set") {
        n_notifications += 1;
        last_data = data.key;
        last_key_raw = data.key_raw;
    }
});

redis.register_function("notifications_stats", async function(){
    return [
        n_notifications,
        last_key,
        last_key_raw
    ];
});
    """
    env.expect('RG.FCALL', 'lib', 'notifications_stats', '0').equal([0, None, None])
    env.expect('SET', b'\xff\xff', b'\xaa').equal(True)
    env.expect('RG.FCALL', 'lib', 'notifications_stats', '0').equal([1, None, b'\xff\xff'])
