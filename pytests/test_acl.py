from common import gearsTest
from common import toDictionary
from common import runUntil

NO_PERMISSIONS_ERROR_MSG = 'No permissions to access a key'

@gearsTest()
def testAclOnSyncFunction(env):
    """#!js api_version=1.0 name=lib
redis.register_function("get", function(client, dummy, key){
    return client.call('get', key);
})
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcall').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FCALL', 'lib', 'get', '1', 'x', 'x').equal('1')
    env.expect('RG.FCALL', 'lib', 'get', '1', 'x', 'cached:x').equal('1')
    env.expect('AUTH', 'alice', 'pass').equal(True)
    env.expect('RG.FCALL', 'lib', 'get', '1', 'cached:x', 'x').error().contains(NO_PERMISSIONS_ERROR_MSG)
    env.expect('RG.FCALL', 'lib', 'get', '1', 'cached:x', 'cached:x').equal('1')

@gearsTest()
def testAclOnAsyncFunction(env):
    """#!js api_version=1.0 name=lib
redis.register_async_function("get", async function(client, dummy, key){
    return client.block(function(client){
        return client.call('get', key);
    });
})
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcallasync').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'x', 'x').equal('1')
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'x', 'cached:x').equal('1')
    env.expect('AUTH', 'alice', 'pass').equal(True)
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'cached:x', 'x').error().contains(NO_PERMISSIONS_ERROR_MSG)
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'cached:x', 'cached:x').equal('1')

@gearsTest()
def testAclOnAsyncComplex(env):
    """#!js api_version=1.0 name=lib
redis.register_async_function("get", async function(client, dummy, key){
    return client.block(function(client){
        return client.run_on_background(async function(client) {
            return client.block(function(client) {
                return client.call('get', key);
            });
        });
    });
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcallasync').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'x', 'x').equal('1')
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'x', 'cached:x').equal('1')
    env.expect('AUTH', 'alice', 'pass').equal(True)
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'cached:x', 'x').error().contains(NO_PERMISSIONS_ERROR_MSG)
    env.expect('RG.FCALLASYNC', 'lib', 'get', '1', 'cached:x', 'cached:x').equal('1')

@gearsTest()
def testAclUserDeletedWhileFunctionIsRunning(env):
    """#!js api_version=1.0 name=lib
var async_get_continue = null;
var async_get_resolve = null;
var async_get_reject = null;

redis.register_async_function("async_get_continue", async function(client){
    async_get_continue("continue");
    return await new Promise((resolve, reject) => {
        async_get_resolve = resolve;
        async_get_reject = reject;
    })
});

redis.register_function("async_get_start", function(client, dummy, key){
    client.run_on_background(async function(client) {
        await new Promise((resolve, reject) => {
            async_get_continue = resolve;
        });
        client.block(function(client){
            try {
                async_get_resolve(client.call('get', key));
            } catch (e) {
                async_get_reject(e);
            }
        });
    });
    return "OK";
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcallasync').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FCALLASYNC', 'lib', 'async_get_start', '1', 'cached:x', 'x').equal('OK')
    env.expect('RG.FCALLASYNC', 'lib', 'async_get_continue', '0').equal('1')
    env.expect('RG.FCALLASYNC', 'lib', 'async_get_start', '1', 'cached:x', 'cached:x').equal('OK')
    env.expect('RG.FCALLASYNC', 'lib', 'async_get_continue', '0').equal('1')
    c = env.getConnection()
    c.execute_command('AUTH', 'alice', 'pass')

    env.assertEqual(c.execute_command('RG.FCALLASYNC', 'lib', 'async_get_start', '1', 'cached:x', 'x'), "OK")
    try:
        c.execute_command('RG.FCALLASYNC', 'lib', 'async_get_continue', '0')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains(NO_PERMISSIONS_ERROR_MSG, str(e))

    env.assertEqual(c.execute_command('RG.FCALLASYNC', 'lib', 'async_get_start', '1', 'cached:x', 'cached:x'), "OK")
    try:
        env.assertEqual(c.execute_command('RG.FCALLASYNC', 'lib', 'async_get_continue', '0'), '1')
    except Exception as e:
        env.assertTrue(False, message='Command failed though should success, %s' % str(e))

    env.assertEqual(c.execute_command('RG.FCALLASYNC', 'lib', 'async_get_start', '1', 'cached:x', 'cached:x'), "OK")
    env.expect('ACL', 'DELUSER', 'alice').equal(1) # delete alice user while function is running
    try:
        c.execute_command('RG.FCALLASYNC', 'lib', 'async_get_continue', '0')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains("User does not exists or disabled", str(e))

@gearsTest()
def testAclOnNotificationConsumer(env):
    script = """#!js api_version=1.0 name=lib
redis.register_notifications_consumer("test", "", function(client, data) {
    return client.call("get", "x");
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcall').equal('OK')
    c = env.getConnection()
    c.execute_command('AUTH', 'alice', 'pass')
    c.execute_command('RG.FUNCTION', 'LOAD', script)
    user = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['user']
    env.assertEqual(user, 'alice')
    env.expect('set', 'x', '1').equal(True)
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['triggers'][0]['last_error']
    env.assertContains('User does not have permissions on key', last_error)
    env.expect('set', 'cached:x', '1').equal(True)
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['triggers'][0]['last_error']
    env.assertContains(NO_PERMISSIONS_ERROR_MSG, last_error)

@gearsTest()
def testAclOnAsyncNotificationConsumer(env):
    script = """#!js api_version=1.0 name=lib
redis.register_notifications_consumer("test", "", async function(client, data) {
    client.block(function(c){
        return c.call("get", "x");
    });
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcall').equal('OK')
    c = env.getConnection()
    c.execute_command('AUTH', 'alice', 'pass')
    c.execute_command('RG.FUNCTION', 'LOAD', script)
    user = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['user']
    env.assertEqual(user, 'alice')

    env.expect('set', 'x', '1').equal(True)
    runUntil(env, 1, lambda: toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['triggers'][0]['num_failed'])
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['triggers'][0]['last_error']
    env.assertContains('User does not have permissions on key', last_error)

    env.expect('set', 'cached:x', '1').equal(True)
    runUntil(env, 2, lambda: toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['triggers'][0]['num_failed'])
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['triggers'][0]['last_error']
    env.assertContains(NO_PERMISSIONS_ERROR_MSG, last_error)

@gearsTest()
def testAclOnStreamConsumer(env):
    script = """#!js api_version=1.0 name=lib
redis.register_stream_consumer("consumer", "", 1, false, function(client){
    return client.call("get", "x");
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcall').equal('OK')
    c = env.getConnection()
    c.execute_command('AUTH', 'alice', 'pass')
    c.execute_command('RG.FUNCTION', 'LOAD', script)
    user = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['user']
    env.assertEqual(user, 'alice')
    env.cmd('xadd', 's', '*', 'foo', 'bar')
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['last_error']
    env.assertContains('User does not have permissions on key', last_error)

    env.cmd('del', 's') # delete the stream, we want to have a single stream for tests simplicity.

    env.cmd('xadd', 'cached:x', '*', 'foo', 'bar')
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['last_error']
    env.assertContains(NO_PERMISSIONS_ERROR_MSG, last_error)

@gearsTest()
def testAclOnAsyncStreamConsumer(env):
    script = """#!js api_version=1.0 name=lib
redis.register_stream_consumer("consumer", "", 1, false, async function(client){
    return client.block(function(c) {
        return c.call("get", "x");
    });
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function', '+rg.fcall').equal('OK')
    c = env.getConnection()
    c.execute_command('AUTH', 'alice', 'pass')
    c.execute_command('RG.FUNCTION', 'LOAD', script)
    user = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['user']
    env.assertEqual(user, 'alice')

    env.cmd('xadd', 's', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['last_error']
    env.assertContains('User does not have permissions on key', last_error)

    env.cmd('del', 's') # delete the stream, we want to have a single stream for tests simplicity.

    env.cmd('xadd', 'cached:x', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    last_error = toDictionary(env.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['last_error']
    env.assertContains(NO_PERMISSIONS_ERROR_MSG, last_error)

