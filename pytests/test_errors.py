from common import gearsTest

@gearsTest()
def testWrongEngine(env):
    script = '''#!js1 name=foo
redis.register_function("test", function(client){
    return 2
})  
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('Unknown backend')

@gearsTest()
def testNoName(env):
    script = '''#!js
redis.register_function("test", function(client){
    return 2
})  
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Failed find 'name' property")

@gearsTest()
def testSameFunctionName(env):
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})
redis.register_function("test", function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Function test already exists")

@gearsTest()
def testWrongArguments1(env):
    script = '''#!js name=foo
redis.register_function(1, function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("must be a string")

@gearsTest()
def testWrongArguments2(env):
    script = '''#!js name=foo
redis.register_function("test", "foo")
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("must be a function")

@gearsTest()
def testNoRegistrations(env):
    script = '''#!js name=foo

    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("No function nor registrations was registered")

@gearsTest()
def testBlockRedisTwice(env):
    """#!js name=foo
redis.register_function('test', async function(c1){
    return await c1.block(function(c2){
        c1.block(function(c3){}); // blocking again
    });
})
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('thread is already blocked')
    
@gearsTest()
def testCallRedisWhenNotBlocked(env):
    """#!js name=foo
redis.register_function('test', async function(c){
    return await c.block(function(c1){
        return c1.run_on_background(async function(c2){
            return c1.call('ping'); // call redis when not blocked
        });
    });
})
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('thread is not locked')
    
@gearsTest()
def testCommandsNotAllowedOnScript(env):
    """#!js name=foo
redis.register_function('test1', function(c){
    return c.call('eval', 'return 1', '0');
})
redis.register_function('test2', async function(c1){
    c1.block(function(c2){
        return c2.call('eval', 'return 1', '0');
    });
})
    """
    env.expect('RG.FCALL', 'foo', 'test1', '0').error().contains('is not allowed on script mode')
    env.expect('RG.FCALL', 'foo', 'test2', '0').error().contains('is not allowed on script mode')

@gearsTest()
def testJSStackOverflow(env):
    """#!js name=foo
function test() {
    test();
}
redis.register_function('test', test);
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('Maximum call stack size exceeded')

@gearsTest()
def testJSStackOverflowOnLoading(env):
    script = """#!js name=foo
function test(i) {
    redis.log(JSON.stringify(i))
    test(i+1);
}
test(1);
redis.register_function('test', test);
    """
    env.expect('CONFIG', 'SET', 'redisgears_2.lock-redis-timeout', '10000')
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Maximum call stack size exceeded")

@gearsTest()
def testMissingConfig(env):
    script = """#!js name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG').error().contains("configuration value was not given")

@gearsTest()
def testNoJsonConfig(env):
    script = """#!js name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', 'foo').error().contains("configuration must be a valid json")

@gearsTest()
def testNoJsonObjectConfig(env):
    script = """#!js name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', '5').error().contains("configuration must be a valid json object")

@gearsTest()
def testLongNestedReply(env):
    """#!js name=foo
function test() {
    var a = [];
    a[0] = a;
    return a;
}
redis.register_function('test', test);
    """
    env.expect('RG.FCALL', 'foo', 'test', '0')
    env.expect('PING').equal(True)

@gearsTest()
def testFcallWithWrangArgumets(env):
    """#!js name=foo
function test() {
    return 'test';
}
redis.register_function('test', test);
    """
    env.expect('RG.FCALL', 'foo', 'test', '10', 'bar').error().contains('Not enough arguments was given')
    env.expect('RG.FCALL', 'foo', 'test').error().contains('wrong number of arguments ')

@gearsTest()
def testNotExistsRemoteFunction(env):
    """#!js name=foo
redis.register_function("test", async (async_client) => {
    return await async_client.run_on_key('x', 'not_exists');
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('Remote function not_exists does not exists')

@gearsTest()
def testRemoteFunctionNotSerializableInput(env):
    """#!js name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call("get", key);
    });
    return res;
});

redis.register_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, ()=>{return 1;});
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '1', '1').error().contains('Failed deserializing remote function argument')

@gearsTest()
def testRemoteFunctionNotSerializableOutput(env):
    """#!js name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    return ()=>{return 1;};
});

redis.register_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '1', '1').error().contains('Failed deserializing remote function result')

@gearsTest()
def testRegisterRemoteFunctionWorngNumberOfArgs(env):
    script = """#!js name=foo
redis.register_remote_function();
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Wrong number of arguments to 'register_remote_function' function")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType(env):
    script = """#!js name=foo
redis.register_remote_function(1, async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("First argument to 'register_remote_function' must be a string")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType2(env):
    script = """#!js name=foo
redis.register_remote_function('test', 'test');
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Second argument to 'register_remote_function' must be a function")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType3(env):
    script = """#!js name=foo
redis.register_remote_function('test', (async_client, key) => {
    return async_client.run_on_key(key, remote_get, key);
});
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Remote function must be async")

@gearsTest()
def testRedisAITensorCreateWithoutRedisAI(env):
    """#!js name=foo
redis.register_function("test", (client) => {
    return redis.redisai.create_tensor("FLOAT", [1, 3], new Uint8Array(16).buffer);
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('RedisAI is not initialize')

@gearsTest()
def testRedisAIModelCreateWithoutRedisAI(env):
    """#!js name=foo
redis.register_function("test", (client) => {
    return client.redisai.open_model("foo");
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('RedisAI is not initialize')

@gearsTest()
def testRedisAIScriptCreateWithoutRedisAI(env):
    """#!js name=foo
redis.register_function("test", (client) => {
    return client.redisai.open_script("foo");
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('RedisAI is not initialize')

@gearsTest()
def testUseOfInvalidClient(env):
    """#!js name=foo
redis.register_function("test", (client) => {
    return client.run_on_background(async (async_client) => {
        return async_client.block(()=>{
            return client.call("ping");
        });
    });
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('Used on invalid client')

@gearsTest()
def testCallWithoutBlock(env):
    """#!js name=foo
redis.register_function("test", (client) => {
    return client.run_on_background(async () => {
            return client.call("ping");
    });
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('Main thread is not locked')
