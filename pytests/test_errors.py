from common import gearsTest

@gearsTest()
def testWrongEngine(env):
    script = '''#!js1 api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 2
})
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains('Unknown backend')

@gearsTest()
def testNoApiVersion(env):
    script = '''#!js name=foo
redis.registerFunction("test", function(client){
    return 2
})
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("The api version is missing from the prologue.")

@gearsTest()
def testNoName(env):
    script = '''#!js api_version=1.0
redis.registerFunction("test", function(client){
    return 2
})
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("The library name is missing from the prologue.")

@gearsTest()
def testSameFunctionName(env):
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 2
})
redis.registerFunction("test", function(client){
    return 2
})
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("Function test already exists")

@gearsTest()
def testWrongArguments1(env):
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction(1, function(client){
    return 2
})
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("Value is not string")

@gearsTest()
def testWrongArguments2(env):
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction("test", "foo")
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("must be a function")

@gearsTest()
def testNoRegistrations(env):
    script = '''#!js api_version=1.0 name=foo

    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("Neither function nor other registrations were found")

@gearsTest()
def testBlockRedisTwice(env):
    """#!js api_version=1.0 name=foo
redis.registerAsyncFunction('test', async function(c1){
    return await c1.block(function(c2){
        c1.block(function(c3){}); // blocking again
    });
})
    """
    env.expectTfcallAsync('foo', 'test').error().contains('thread is already blocked')

@gearsTest()
def testCallRedisWhenNotBlocked(env):
    """#!js api_version=1.0 name=foo
redis.registerAsyncFunction('test', async function(c){
    return await c.block(function(c1){
        return c1.executeAsync(async function(c2){
            return c1.call('ping'); // call redis when not blocked
        });
    });
})
    """
    env.expectTfcallAsync('foo', 'test').error().contains('thread is not locked')

@gearsTest()
def testCommandsNotAllowedOnScript(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction('test1', function(c){
    return c.call('eval', 'return 1', '0');
})
redis.registerAsyncFunction('test2', async function(c1){
    c1.block(function(c2){
        return c2.call('eval', 'return 1', '0');
    });
})
    """
    env.expectTfcall('foo', 'test1').error().contains('is not allowed on script mode')
    env.expectTfcallAsync('foo', 'test2').error().contains('is not allowed on script mode')

@gearsTest(gearsConfig={"v8-flags": "'--stack-size=50'"})
def testJSStackOverflow(env):
    """#!js api_version=1.0 name=foo
function test() {
    test();
}
redis.registerFunction('test', test);
    """
    env.expectTfcall('foo', 'test').error().contains('Maximum call stack size exceeded')

@gearsTest(gearsConfig={"v8-flags": "'--stack-size=50'"})
def testJSStackOverflowOnLoading(env):
    script = """#!js api_version=1.0 name=foo
function test(i) {
    test(i+1);
}
test(1);
redis.registerFunction('test', test);
    """
    env.expect('CONFIG', 'SET', 'redisgears_2.lock-redis-timeout', '10000')
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains("Maximum call stack size exceeded")

@gearsTest()
def testMissingConfig(env):
    script = """#!js api_version=1.0 name=foo
function test() {
    test();
}
test();
redis.registerFunction('test', test);
    """
    env.expect('TFUNCTION', 'LOAD', 'CONFIG').error().contains("configuration value was not given")

@gearsTest()
def testNoJsonConfig(env):
    script = """#!js api_version=1.0 name=foo
function test() {
    test();
}
test();
redis.registerFunction('test', test);
    """
    env.expect('TFUNCTION', 'LOAD', 'CONFIG', 'foo').error().contains("configuration must be a valid json")

@gearsTest()
def testNoJsonObjectConfig(env):
    script = """#!js api_version=1.0 name=foo
function test() {
    test();
}
test();
redis.registerFunction('test', test);
    """
    env.expect('TFUNCTION', 'LOAD', 'CONFIG', '5').error().contains("configuration must be a valid json object")

@gearsTest()
def testLongNestedReply(env):
    """#!js api_version=1.0 name=foo
function test() {
    var a = [];
    a[0] = a;
    return a;
}
redis.registerFunction('test', test);
    """
    env.expectTfcall('foo', 'test')
    env.expect('PING').equal(True)

@gearsTest()
def testFcallWithWrangArgumets(env):
    """#!js api_version=1.0 name=foo
function test() {
    return 'test';
}
redis.registerFunction('test', test);
    """
    env.expect('TFCALL', 'foo.test', '10', 'bar').error().contains('Not enough arguments was given')
    env.expect('TFCALL', 'foo.test').error().contains('wrong number of arguments ')

@gearsTest()
def testNotExistsRemoteFunction(env):
    """#!js api_version=1.0 name=foo
redis.registerAsyncFunction("test", async (async_client) => {
    return await async_client.runOnKey('x', 'not_exists');
});
    """
    env.expectTfcallAsync('foo', 'test').error().contains('Remote function not_exists does not exists')

@gearsTest()
def testRemoteFunctionNotSerializableInput(env):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.registerClusterFunction(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call("get", key);
    });
    return res;
});

redis.registerAsyncFunction("test", async (async_client, key) => {
    return await async_client.runOnKey(key, remote_get, ()=>{return 1;});
});
    """
    env.expectTfcallAsync('foo', 'test', ['1']).error().contains('Failed deserializing remote function argument')

@gearsTest()
def testRemoteFunctionNotSerializableOutput(env):
    """#!js api_version=1.0 name=foo
const remote_get = "remote_get";

redis.registerClusterFunction(remote_get, async(client, key) => {
    return ()=>{return 1;};
});

redis.registerAsyncFunction("test", async (async_client, key) => {
    return await async_client.runOnKey(key, remote_get, key);
});
    """
    env.expectTfcallAsync('foo', 'test', ['1']).error().contains('Failed deserializing remote function result')

@gearsTest()
def testRegisterRemoteFunctionWorngNumberOfArgs(env):
    script = """#!js api_version=1.0 name=foo
redis.registerClusterFunction();
    """
    env.expect('TFUNCTION', 'LOAD', script).error().contains("Wrong number of arguments given")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType(env):
    script = """#!js api_version=1.0 name=foo
redis.registerClusterFunction(1, async (async_client, key) => {
    return await async_client.runOnKey(key, remote_get, key);
});
    """
    env.expect('TFUNCTION', 'LOAD', script).error().contains("Value is not string")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType2(env):
    script = """#!js api_version=1.0 name=foo
redis.registerClusterFunction('test', 'test');
    """
    env.expect('TFUNCTION', 'LOAD', script).error().contains("Second argument to 'registerClusterFunction' must be a function")

@gearsTest()
def testRegisterRemoteFunctionWorngfArgsType3(env):
    script = """#!js api_version=1.0 name=foo
redis.registerClusterFunction('test', (async_client, key) => {
    return async_client.runOnKey(key, remote_get, key);
});
    """
    env.expect('TFUNCTION', 'LOAD', script).error().contains("Remote function must be async")

@gearsTest()
def testRedisAITensorCreateWithoutRedisAI(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", (client) => {
    return redis.redisai.create_tensor("FLOAT", [1, 3], new Uint8Array(16).buffer);
});
    """
    env.expectTfcall('foo', 'test').error().contains('RedisAI is not initialize')

@gearsTest()
def testRedisAIModelCreateWithoutRedisAI(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", (client) => {
    return client.redisai.open_model("foo");
});
    """
    env.expectTfcall('foo', 'test').error().contains('RedisAI is not initialize')

@gearsTest()
def testRedisAIScriptCreateWithoutRedisAI(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", (client) => {
    return client.redisai.open_script("foo");
});
    """
    env.expectTfcall('foo', 'test').error().contains('RedisAI is not initialize')

@gearsTest()
def testUseOfInvalidClient(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", (client) => {
    return client.executeAsync(async (async_client) => {
        return async_client.block(()=>{
            return client.call("ping");
        });
    });
});
    """
    env.expectTfcallAsync('foo', 'test').error().contains('Used on invalid client')

@gearsTest()
def testCallWithoutBlock(env):
    """#!js api_version=1.0 name=foo
redis.registerFunction("test", (client) => {
    return client.executeAsync(async () => {
            return client.call("ping");
    });
});
    """
    env.expectTfcallAsync('foo', 'test').error().contains('Main thread is not locked')

@gearsTest()
def testDelNoneExistingFunction(env):
    env.expect('TFUNCTION', 'DELETE', 'FOO').error().contains('library does not exists')

@gearsTest()
def testFunctionListWithBinaryOption(env):
    env.expect('TFUNCTION', 'LIST', b'\xaa').error().contains('Binary option is not allowed')

@gearsTest()
def testFunctionListWithBinaryLibraryName(env):
    env.expect('TFUNCTION', 'LIST', 'LIBRARY', b'\xaa').error().contains('Library name is not a string')

@gearsTest()
def testFunctionListWithUngivenLibraryName(env):
    env.expect('TFUNCTION', 'LIST', 'LIBRARY').error().contains('Library name was not given')

@gearsTest()
def testFunctionListWithUnknownOption(env):
    env.expect('TFUNCTION', 'LIST', 'FOO').error().contains('Unknown option')

@gearsTest()
def testMalformedLibraryMetaData(env):
    code = 'js api_version=1.0 name=foo' # no shebang(#!)
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Invalid or missing prologue.')

@gearsTest()
def testMalformedLibraryMetaData2(env):
    code = '#!js name' # no api version and no library version, invalid prologue.
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Invalid or missing prologue.')

@gearsTest()
def testMalformedLibraryMetaData3(env):
    code = '#!js api_version=1.0 foo=bar' # unknown property
    env.expect('TFUNCTION', 'LOAD', code).error().contains('The library name is missing from the prologue.')

@gearsTest()
def testMalformedLibraryMetaData4(env):
    code = '#!js api_version=1.0 name=foo foo=bar xxx=yyy' # unknown properties
    error = env.expect('TFUNCTION', 'LOAD', code).error()
    error.contains('"foo"')
    error.contains('"xxx"')

@gearsTest()
def testMalformedLibraryMetaData5(env):
    code = '#!js name=foo name=bar' # duplicated property name
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Duplicated prologue properties found: name')

@gearsTest()
def testMalformedLibraryMetaData6(env):
    code = '#!js api_version=1.0 name=foo x' # invalid syntax
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Invalid or missing prologue.')

@gearsTest()
def testMalformedLibraryMetaData7(env):
    code = '#!js name=foo' # no API version
    env.expect('TFUNCTION', 'LOAD', code).error().contains('The api version is missing from the prologue.')


@gearsTest()
def testNoLibraryCode(env):
    env.expect('TFUNCTION', 'LOAD').error().contains('Could not find library payload')

@gearsTest()
def testNoValidJsonConfig(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expect('TFUNCTION', 'LOAD', 'CONFIG', b'\xaa', code).error().contains("given configuration value is not a valid string")

@gearsTest()
def testSetUserAsArgument(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expect('TFUNCTION', 'LOAD', 'USER', 'foo', code).error().contains("Unknown argument user")

@gearsTest()
def testBinaryLibCode(env):
    env.expect('TFUNCTION', 'LOAD', b'\xaa').error().contains("lib code must a valid string")


@gearsTest()
def testUploadSameLibraryName(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expect('TFUNCTION', 'LOAD', code).equal('OK')
    env.expect('TFUNCTION', 'LOAD', code).error().contains('already exists')

@gearsTest()
def testUnknownFunctionSubCommand(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expect('TFUNCTION', 'Foo').error().contains('Unknown subcommand')

@gearsTest()
def testUnknownFunctionName(env):
    '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expectTfcall('lib', 'foo').error().contains("Unknown function")

@gearsTest()
def testCallFunctionOnOOM(env):
    '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expect('config', 'set', 'maxmemory', '1').equal('OK')
    env.expectTfcallAsync('lib', 'test').error().contains("OOM can not run the function when out of memory")

@gearsTest()
def testRegisterSameConsumerTwice(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerKeySpaceTrigger("consumer", "key", async function(client, data) {
    client.block(function(client){
        client.call('incr', 'count')
    });
});

redis.registerKeySpaceTrigger("consumer", "key", async function(client, data) {
    client.block(function(client){
        client.call('incr', 'count')
    });
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Notification consumer already exists')

@gearsTest()
def testRegisterSameStreamConsumerTwice(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "stream", function(){
    return 0;
});
redis.registerStreamTrigger("consumer", "stream", function(){
    return 0;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Stream registration already exists')

@gearsTest()
def testUpgradeStreamConsumerWithDifferentPrefix(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerStreamTrigger("consumer", "%s", function(){
    return 0;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code % 'prefix1').equal('OK')
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', code % 'prefix2').error().contains('Can not upgrade an existing consumer with different prefix')

@gearsTest()
def testRegisterSameRemoteTaskTwice(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerClusterFunction("remote", async(client, key) => {
    return 1;
});
redis.registerClusterFunction("remote", async(client, key) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('already exists')

@gearsTest()
def testWrongFlagValue(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1}, [1])
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unknown properties given: 0')

@gearsTest()
def testUnknownFlagValue(env):
    code = '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1}, {flags:["unknown"]})
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unknow flag')

@gearsTest()
def testArgDecodeFailure(env):
    '''#!js api_version=1.0 name=lib
redis.registerFunction('test', () => {return 1})
    '''
    env.expectTfcall('lib', 'test', [], [b'\xaa']).error().contains('Can not convert argument to string')

@gearsTest()
def testArgDecodeFailureAsync(env):
    '''#!js api_version=1.0 name=lib
redis.registerAsyncFunction('test', async () => {return 1})
    '''
    env.expectTfcallAsync('lib', 'test', [], [b'\xaa']).error().contains('Can not convert argument to string')

@gearsTest()
def testCallAsyncFunctionWithTFCALL(env):
    '''#!js api_version=1.0 name=lib
redis.registerAsyncFunction('test', async () => {return 1})
    '''
    env.expectTfcall('lib', 'test').error().contains('function is declared as async and was called while blocking was not allowed')

@gearsTest()
def testBlockOnTFCall(env):
    '''#!js api_version=1.0 name=lib
redis.registerFunction('test', (c) => {
    return c.executeAsync(async function(){
        return 1;
    });
});
    '''
    env.expectTfcall('lib', 'test').error().contains('Can not block client for background execution')

@gearsTest()
def testUnallowedLibraryName(env):
    code = '''#!js api_version=1.0 name=foo.bar
redis.registerFunction("test", (client) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unallowed library name \'foo.bar\'')

@gearsTest()
def testUnallowedFunctionName(env):
    code = '''#!js api_version=1.0 name=foo
redis.registerFunction("test.test", (client) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unallowed function name \'test.test\'')

@gearsTest()
def testUnallowedAsyncFunctionName(env):
    code = '''#!js api_version=1.0 name=foo
redis.registerAsyncFunction("test.test", async (client) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unallowed function name \'test.test\'')

@gearsTest()
def testUnallowedStreamTriggerName(env):
    code = '''#!js api_version=1.0 name=foo
redis.registerStreamTrigger("test.test", "stream", async (client) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unallowed stream trigger name \'test.test\'')

@gearsTest()
def testUnallowedKeySpaceTriggerName(env):
    code = '''#!js api_version=1.0 name=foo
redis.registerKeySpaceTrigger("test.test", "key", async (client) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unallowed key space trigger name \'test.test\'')

@gearsTest()
def testUnallowedClusterFunctionName(env):
    code = '''#!js api_version=1.0 name=foo
redis.registerClusterFunction("test.test", async (client) => {
    return 1;
});
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('Unallowed cluster function name \'test.test\'')


@gearsTest()
def testWasmIsNotExposeByDefault(env):
    code = '''#!js api_version=1.0 name=foo
WebAssembly.Global
    '''
    env.expect('TFUNCTION', 'LOAD', code).error().contains('WebAssembly is not defined')
