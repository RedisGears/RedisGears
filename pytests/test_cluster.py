from common import gearsTest
from common import shardsConnections

@gearsTest(cluster=True)
def testBasicClusterSupport(env, cluster_conn):
    """#!js name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call("get", key);
    });
    return res;
});

redis.register_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    cluster_conn.execute_command('set', 'x', '1')
    for conn in shardsConnections(env):
        res = conn.execute_command('RG.FCALL_NO_KEYS', 'foo', 'test', '1', 'x')
        env.assertEqual(res, '1')

@gearsTest(cluster=True)
def testRecursiveLookup(env, cluster_conn):
    """#!js name=foo
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

redis.register_function("test", async(async_client, key) => {
    return await async_client.run_on_key(key, recursive_get, key);
});
    """
    for i in range(1000):
        cluster_conn.execute_command('hset', 'key%d' % i, 'lookup', 'key%d' % (i + 1))
    cluster_conn.execute_command('set', 'key%d' % i, 'final_value')
    for conn in shardsConnections(env):
        res = conn.execute_command('RG.FCALL_NO_KEYS', 'foo', 'test', '1', 'key0')
        env.assertEqual(res, 'final_value')



