from common import gearsTest
from common import toDictionary
from common import runUntil

@gearsTest(enableGearsDebugCommands=True)
def testSimpleCallAsync(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction('test', (c) => {
    return c.executeAsync(async (c) => {
        var res = await c.block((c) => {
            return c.callAsync("blpop", "l", "0");
        });
        c.block((c) => {
            return c.call("lpush", "l1", res[1]);
        });
        return "OK"
    });
});
    """

    future = env.noBlockingTfcallAsync('lib', 'test')
    runUntil(env, 'blpop l 0', lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 2)[0]['pending_async_calls'][0])
    env.expect('lpush', 'l', '1').equal(1)
    runUntil(env, 1, lambda: env.cmd('llen', 'l1'))
    env.expect('lrange', 'l1', '0', '-1').equal(['1'])
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 2)
    env.assertEqual(res[0]['pending_async_calls'], [])
    future.equal("OK")
    # make sure the weak refernce are cleaned.
    runUntil(env, [], lambda: env.cmd('TFUNCTION', 'DEBUG', 'dump_pending_async_calls'))

@gearsTest(decodeResponses=False, enableGearsDebugCommands=True)
def testSimpleCallAsyncRaw(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction('test', (c) => {
    return c.executeAsync(async (c) => {
        var res = await c.block((c) => {
            return c.callAsyncRaw("blpop", "l", "0");
        });
        c.block((c) => {
            return c.call("lpush", "l1", res[1]);
        });
        return "OK"
    });
});
    """
    future = env.noBlockingTfcallAsync('lib', 'test')
    runUntil(env, b'blpop l 0', lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 2)[0][b'pending_async_calls'][0])
    env.expect('lpush', 'l', b'\x00').equal(1)
    runUntil(env, 1, lambda: env.cmd('llen', 'l1'))
    env.expect('lrange', 'l1', '0', '-1').equal([b'\x00'])
    res = toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 2)
    env.assertEqual(res[0][b'pending_async_calls'], [])
    future.equal(b"OK")
    # make sure the weak refernce are cleaned.
    runUntil(env, [], lambda: env.cmd('TFUNCTION', 'DEBUG', 'dump_pending_async_calls'))

@gearsTest(enableGearsDebugCommands=True)
def testCallAsyncWithDirectResult(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction('test', (c) => {
    return c.callAsync("blpop", "l", "0");
});
    """

    env.expect('lpush', 'l', '1').equal(1)
    env.expectTfcallAsync('lib', 'test').equal(['l', '1'])
    # make sure the weak refernce are cleaned.
    runUntil(env, [], lambda: env.cmd('TFUNCTION', 'DEBUG', 'dump_pending_async_calls'))

@gearsTest(enableGearsDebugCommands=True)
def testCallAsyncBecomeReplica(env):
    """#!js api_version=1.0 name=lib
redis.registerAsyncFunction('test', (c) => {
    return c.executeAsync(async (c) => {
        var res = await c.block((c) => {
            return c.callAsync("blpop", "l", "0");
        });
        c.block((c) => {
            return c.call("lpush", "l1", res[1]);
        });
        return "OK"
    });
});
    """

    future = env.noBlockingTfcallAsync('lib', 'test')
    runUntil(env, 'blpop l 0', lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 2)[0]['pending_async_calls'][0])
    env.expect('replicaof', 'localhost', '1111').equal('OK')
    runUntil(env, [], lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vv'), 2)[0]['pending_async_calls'])
    future.expectError('instance state changed')
    # make sure the weak refernce are cleaned.
    runUntil(env, [], lambda: env.cmd('TFUNCTION', 'DEBUG', 'dump_pending_async_calls'))
