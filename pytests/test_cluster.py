from common import gearsTest
from common import shardsConnections
from common import failTest

@gearsTest(cluster=True)
def testBasicClusterSupport(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call("get", key);
    });
    return res;
});

redis.register_async_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    cluster_conn.execute_command('set', 'x', '1')
    for conn in shardsConnections(env):
        res = conn.execute_command('RG.FCALLASYNC', 'foo', 'test', '1', 'x')
        env.assertEqual(res, '1')

@gearsTest(cluster=True)
def testBasicClusterBinaryInputOutputSupport(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call_raw("get", key);
    });
    return res;
});

redis.register_async_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
},
["raw-arguments"]);
    """
    cluster_conn.execute_command('set', 'x', '1')
    for conn in shardsConnections(env):
        res = conn.execute_command('RG.FCALLASYNC', 'foo', 'test', '1', 'x')
        env.assertEqual(res, '1')

@gearsTest(cluster=True)
def testRemoteFunctionRaiseError(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    throw 'Remote function failure';
});

redis.register_async_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
},
["raw-arguments"]);
    """
    cluster_conn.execute_command('set', 'x', '1')
    for conn in shardsConnections(env):
        try:
            conn.execute_command('RG.FCALLASYNC', 'foo', 'test', '1', 'x')
            pass
        except Exception as e:
            env.assertContains('Remote function failure', str(e))
            continue
        failTest(env, 'error was not raised by command')

@gearsTest(cluster=True)
def testRecursiveLookup(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const recursive_get = "recursive_get";

redis.register_remote_function(recursive_get, async(client, key) => {
    return await client.block((client) => {
        let t = client.call("type", key);
        if (t == 'string') {
            return client.call("get", key);
        }
        let next_key = client.call("hget", key, 'lookup');
        return client.run_on_background(async(client) => {
            return await client.run_on_key(next_key, recursive_get, next_key);
        });
    });
});

redis.register_async_function("test", async(async_client, key) => {
    return await async_client.run_on_key(key, recursive_get, key);
});
    """
    for i in range(100):
        cluster_conn.execute_command('hset', 'key%d' % i, 'lookup', 'key%d' % (i + 1))
    cluster_conn.execute_command('set', 'key%d' % i, 'final_value')
    for conn in shardsConnections(env):
        res = conn.execute_command('RG.FCALLASYNC', 'foo', 'test', '1', 'key0')
        env.assertEqual(res, 'final_value')

@gearsTest(cluster=True, gearsConfig={'remote-task-default-timeout': '1'})
def testRemoteTaskTimeout(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    while (true); // run forever so we will get the timeout.
    return 'done';
});

redis.register_async_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    cluster_conn.execute_command('set', 'x', '1')
    env.expect('RG.FCALLASYNC', 'foo', 'test', '1', 'x').error().contains('Remote task timeout')

@gearsTest(cluster=True)
def testRunOnAllShards(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const dbside_remote_func = "dbsize";

redis.register_remote_function(dbside_remote_func, async(client) => {
    return await client.block((client) => {
        return client.call("dbsize").toString();
    });
});

redis.register_async_function("test", async(async_client) => {
    let res = await async_client.run_on_all_shards(dbside_remote_func);
    let results = res[0];
    let errors = res[1];
    if (errors.length > 0) {
        return errors;
    }
    let sum = BigInt(0);
    results.forEach((element) => sum+=BigInt(element));
    return sum;
});
    """
    for i in range(1000):
        cluster_conn.execute_command('set', 'key%d' % i, '1')
    for conn in shardsConnections(env):
        res = conn.execute_command('RG.FCALLASYNC', 'foo', 'test', '0')
        env.assertEqual(res, 1000)

@gearsTest(cluster=True, gearsConfig={'remote-task-default-timeout': '1'})
def testRunOnAllShardsTimeout(env, cluster_conn):
    """#!js api_version=1.0 name=foo
const remote_function = "remote_function";

redis.register_remote_function(remote_function, async(client, key) => {
    let val = client.block((client) => {
        try {
            return client.call("get", "z");
        } catch(e) {
            return 0;
        }
    });
    if (val != "1") {
        while(true); // block forever so we will get a timeout
    }
    return val;
});

redis.register_async_function("test", async (async_client) => {
    let res = await async_client.run_on_all_shards(remote_function);
    if (res[1].length > 0) {
        throw res[1][0];
    }
    return res[0];
});
    """
    cluster_conn.execute_command('set', 'z', '1')
    env.expect('RG.FCALLASYNC', 'foo', 'test', '1', 'z').error().contains('Timeout')
