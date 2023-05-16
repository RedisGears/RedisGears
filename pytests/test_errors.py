from common import gearsTest

@gearsTest()
def testWrongEngine(env):
    script = '''#!js1 api_version=1.0 name=foo
redis.register_function("test", function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('Unknown backend')

@gearsTest()
def testNoApiVersion(env):
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("The api version is missing from the prologue.")

@gearsTest()
def testNoName(env):
    script = '''#!js api_version=1.0
redis.register_function("test", function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("The library name is missing from the prologue.")

@gearsTest()
def testSameFunctionName(env):
    script = '''#!js api_version=1.0 name=foo
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
    script = '''#!js api_version=1.0 name=foo
redis.register_function(1, function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Value is not string")

@gearsTest()
def testWrongArguments2(env):
    script = '''#!js api_version=1.0 name=foo
redis.register_function("test", "foo")
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("must be a function")

@gearsTest()
def testNoRegistrations(env):
    script = '''#!js api_version=1.0 name=foo

    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("No function nor registrations was registered")

@gearsTest()
def testBlockRedisTwice(env):
    """#!js api_version=1.0 name=foo
redis.register_async_function('test', async function(c1){
    return await c1.block(function(c2){
        c1.block(function(c3){}); // blocking again
    });
})
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '0').error().contains('thread is already blocked')

@gearsTest()
def testCallRedisWhenNotBlocked(env):
    """#!js api_version=1.0 name=foo
redis.register_async_function('test', async function(c){
    return await c.block(function(c1){
        return c1.run_on_background(async function(c2){
            return c1.call('ping'); // call redis when not blocked
        });
    });
})
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '0').error().contains('thread is not locked')

@gearsTest()
def testCommandsNotAllowedOnScript(env):
    """#!js api_version=1.0 name=foo
redis.register_function('test1', function(c){
    return c.call('eval', 'return 1', '0');
})
redis.register_async_function('test2', async function(c1){
    c1.block(function(c2){
        return c2.call('eval', 'return 1', '0');
    });
})
    """
    env.expect('RG.FCALL', 'foo', 'test1', '0').error().contains('is not allowed on script mode')
    env.expect('RG.FCALLASYNC', 'foo', 'test2', '0').error().contains('is not allowed on script mode')

@gearsTest()
def testJSStackOverflow(env):
    """#!js api_version=1.0 name=foo
function test() {
    test();
}
redis.register_function('test', test);
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('Maximum call stack size exceeded')

@gearsTest()
def testJSStackOverflowOnLoading(env):
    script = """#!js api_version=1.0 name=foo
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
    script = """#!js api_version=1.0 name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG').error().contains("configuration value was not given")

@gearsTest()
def testNoJsonConfig(env):
    script = """#!js api_version=1.0 name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', 'foo').error().contains("configuration must be a valid json")

@gearsTest()
def testNoJsonObjectConfig(env):
    script = """#!js api_version=1.0 name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', '5').error().contains("configuration must be a valid json object")

@gearsTest()
def testLongNestedReply(env):
    """#!js api_version=1.0 name=foo
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
    """#!js api_version=1.0 name=foo
function test() {
    return 'test';
}
redis.register_function('test', test);
    """
    env.expect('RG.FCALL', 'foo', 'test', '10', 'bar').error().contains('Not enough arguments was given')
    env.expect('RG.FCALL', 'foo', 'test').error().contains('wrong number of arguments ')

@gearsTest()
def testNotExistsRemoteFunction(env):
    """#!js api_version=1.0 name=foo
redis.register_async_function("test", async (async_client) => {
    return await async_client.run_on_key('x', 'not_exists');
});
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '0').error().contains('Remote function not_exists does not exists')

@gearsTest()
def testRemoteFunctionNotSerializableInput(env):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call("get", key);
    });
    return res;
});

redis.register_async_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, ()=>{return 1;});
});
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '1', '1').error().contains('Failed deserializing remote function argument')

@gearsTest()
def testRemoteFunctionNotSerializableOutput(env):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.register_remote_function(remote_get, async(client, key) => {
    return ()=>{return 1;};
});

redis.register_async_function("test", async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '1', '1').error().contains('Failed deserializing remote function result')

@gearsTest()
def testRegisterRemoteFunctionWorngNumberOfArgs(env):
    script = """#!js api_version=1.0 name=foo
redis.register_remote_function();
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Wrong number of arguments given")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType(env):
    script = """#!js api_version=1.0 name=foo
redis.register_remote_function(1, async (async_client, key) => {
    return await async_client.run_on_key(key, remote_get, key);
});
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Value is not string")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType2(env):
    script = """#!js api_version=1.0 name=foo
redis.register_remote_function('test', 'test');
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Second argument to 'register_remote_function' must be a function")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType3(env):
    script = """#!js api_version=1.0 name=foo
redis.register_remote_function('test', (async_client, key) => {
    return async_client.run_on_key(key, remote_get, key);
});
    """
    env.expect('RG.FUNCTION', 'LOAD', script).error().contains("Remote function must be async")

@gearsTest()
def testRedisAITensorCreateWithoutRedisAI(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", (client) => {
    return redis.redisai.create_tensor("FLOAT", [1, 3], new Uint8Array(16).buffer);
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('RedisAI is not initialize')

@gearsTest()
def testRedisAIModelCreateWithoutRedisAI(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", (client) => {
    return client.redisai.open_model("foo");
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('RedisAI is not initialize')

@gearsTest()
def testRedisAIScriptCreateWithoutRedisAI(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", (client) => {
    return client.redisai.open_script("foo");
});
    """
    env.expect('RG.FCALL', 'foo', 'test', '0').error().contains('RedisAI is not initialize')

@gearsTest()
def testUseOfInvalidClient(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", (client) => {
    return client.run_on_background(async (async_client) => {
        return async_client.block(()=>{
            return client.call("ping");
        });
    });
});
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '0').error().contains('Used on invalid client')

@gearsTest()
def testCallWithoutBlock(env):
    """#!js api_version=1.0 name=foo
redis.register_function("test", (client) => {
    return client.run_on_background(async () => {
            return client.call("ping");
    });
});
    """
    env.expect('RG.FCALLASYNC', 'foo', 'test', '0').error().contains('Main thread is not locked')

@gearsTest()
def testDelNoneExistingFunction(env):
    env.expect('RG.FUNCTION', 'DEL', 'FOO').error().contains('library does not exists')

@gearsTest()
def testFunctionListWithBinaryOption(env):
    env.expect('RG.FUNCTION', 'LIST', b'\xaa').error().contains('Binary option is not allowed')

@gearsTest()
def testFunctionListWithBinaryLibraryName(env):
    env.expect('RG.FUNCTION', 'LIST', 'LIBRARY', b'\xaa').error().contains('Library name is not a string')

@gearsTest()
def testFunctionListWithUngivenLibraryName(env):
    env.expect('RG.FUNCTION', 'LIST', 'LIBRARY').error().contains('Library name was not given')

@gearsTest()
def testFunctionListWithUnknownOption(env):
    env.expect('RG.FUNCTION', 'LIST', 'FOO').error().contains('Unknown option')

@gearsTest()
def testMalformedLibraryMetaData(env):
    code = 'js api_version=1.0 name=foo' # no shebang(#!)
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Invalid or missing prologue.')

@gearsTest()
def testMalformedLibraryMetaData2(env):
    code = '#!js name' # no api version and no library version, invalid prologue.
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Invalid or missing prologue.')

@gearsTest()
def testMalformedLibraryMetaData3(env):
    code = '#!js api_version=1.0 foo=bar' # unknown property
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('The library name is missing from the prologue.')

@gearsTest()
def testMalformedLibraryMetaData4(env):
    code = '#!js api_version=1.0 name=foo foo=bar xxx=yyy' # unknown properties
    error = env.expect('RG.FUNCTION', 'LOAD', code).error()
    error.contains('"foo"')
    error.contains('"xxx"')

@gearsTest()
def testMalformedLibraryMetaData5(env):
    code = '#!js name=foo name=bar' # duplicated property name
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Duplicated prologue properties found: name')

@gearsTest()
def testMalformedLibraryMetaData6(env):
    code = '#!js api_version=1.0 name=foo x' # invalid syntax
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Invalid or missing prologue.')

@gearsTest()
def testMalformedLibraryMetaData7(env):
    code = '#!js name=foo' # no API version
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('The api version is missing from the prologue.')


@gearsTest()
def testNoLibraryCode(env):
    env.expect('RG.FUNCTION', 'LOAD').error().contains('Could not find library payload')

@gearsTest()
def testNoValidJsonConfig(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'CONFIG', b'\xaa', code).error().contains("given configuration value is not a valid string")

@gearsTest()
def testSetUserAsArgument(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'USER', 'foo', code).error().contains("Unknown argument user")

@gearsTest()
def testBinaryLibCode(env):
    env.expect('RG.FUNCTION', 'LOAD', b'\xaa').error().contains("lib code must a valid string")


@gearsTest()
def testUploadSameLibraryName(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('RG.FUNCTION', 'LOAD', code).equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('already exists')

@gearsTest()
def testUnknownFunctionSubCommand(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('RG.FUNCTION', 'Foo').error().contains('Unknown subcommand')

@gearsTest()
def testUnknownFunctionName(env):
    '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('RG.FCALL', 'lib', 'foo', '0').error().contains("Unknown function")

@gearsTest()
def testCallFunctionOnOOM(env):
    '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('config', 'set', 'maxmemory', '1').equal('OK')
    env.expect('RG.FCALL', 'lib', 'test', '0').error().contains("OOM can not run the function when out of memory")

@gearsTest()
def testRegisterSameConsumerTwice(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_notifications_consumer("consumer", "key", async function(client, data) {
    client.block(function(client){
        client.call('incr', 'count')
    });
});

redis.register_notifications_consumer("consumer", "key", async function(client, data) {
    client.block(function(client){
        client.call('incr', 'count')
    });
});
    '''
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Notification consumer already exists')

@gearsTest()
def testRegisterSameStreamConsumerTwice(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    return 0;
});
redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    return 0;
});
    '''
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Stream registration already exists')

@gearsTest()
def testUpgradeStreamConsumerWithDifferentPrefix(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_stream_consumer("consumer", "%s", 1, false, function(){
    return 0;
});
    '''
    env.expect('RG.FUNCTION', 'LOAD', code % 'prefix1').equal('OK')
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', code % 'prefix2').error().contains('Can not upgrade an existing consumer with different prefix')

@gearsTest()
def testRegisterSameRemoteTaskTwice(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_remote_function("remote", async(client, key) => {
    return 1;
});
redis.register_remote_function("remote", async(client, key) => {
    return 1;
});
    '''
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('already exists')

@gearsTest()
def testWrongFlagValue(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1}, [1])
    '''
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('wrong type of string value')

@gearsTest()
def testUnknownFlagValue(env):
    code = '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1}, ["unknown"])
    '''
    env.expect('RG.FUNCTION', 'LOAD', code).error().contains('Unknow flag')

@gearsTest()
def testArgDecodeFailure(env):
    '''#!js api_version=1.0 name=lib
redis.register_function('test', () => {return 1})
    '''
    env.expect('RG.FCALL', 'lib', 'test', '0', b'\xaa').error().contains('Can not convert argument to string')

@gearsTest()
def testArgDecodeFailureAsync(env):
    '''#!js api_version=1.0 name=lib
redis.register_async_function('test', async () => {return 1})
    '''
    env.expect('RG.FCALLASYNC', 'lib', 'test', '0', b'\xaa').error().contains('Can not convert argument to string')

@gearsTest()
def testCallAsyncFunctionWithRGFCALL(env):
    '''#!js api_version=1.0 name=lib
redis.register_async_function('test', async () => {return 1})
    '''
    env.expect('RG.FCALL', 'lib', 'test', '0').error().contains('function is declared as async and was called while blocking was not allowed')

@gearsTest()
def testBlockOnRGFCall(env):
    '''#!js api_version=1.0 name=lib
redis.register_function('test', (c) => {
    return c.run_on_background(async function(){
        return 1;
    });
});
    '''
    env.expect('RG.FCALL', 'lib', 'test', '0').error().contains('Can not block client for background execution')
