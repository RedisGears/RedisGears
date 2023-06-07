from common import gearsTest
from common import toDictionary
from common import runUntil

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnFunctionLoad(env):
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return 2
})
foo()
    '''
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', script).error().contains(':5:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnFunctionRun(env):
    '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return foo()
})
    '''
    env.expect('TFCALL', 'test', 0).error().contains(':3:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnAsyncFunctionRun(env):
    '''#!js api_version=1.0 name=foo
redis.registerAsyncFunction("test", async function(client){
    return foo()
})
    '''
    env.expect('TFCALLASYNC', 'test', 0).error().contains(':3:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnFunctionThatReturnsCoro(env):
    '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client){
    return client.executeAsync(async ()=>{
        return foo();
    });
})
    '''
    env.expect('TFCALLASYNC', 'test', 0).error().contains(':4:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnStreamProcessing(env):
    '''#!js api_version=1.0 name=foo
redis.registerStreamTrigger("consumer", "stream", function(c, data){
    return foo()
})
    '''
    env.cmd('xadd', 'stream', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('TFUNCTION', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['stream_triggers'][0]['streams'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnAsyncStreamProcessing(env):
    '''#!js api_version=1.0 name=foo
redis.registerStreamTrigger("consumer", "stream", async function(c, data){
    return foo()
})
    '''
    env.cmd('xadd', 'stream', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['stream_triggers'][0]['streams'][0]['total_record_processed'])
    res = toDictionary(env.cmd('TFUNCTION', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['stream_triggers'][0]['streams'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnNotificationConsumer(env):
    '''#!js api_version=1.0 name=foo
redis.registerKeySpaceTrigger("consumer", "", function(c, data){
    return foo()
})
    '''
    env.cmd('set', 'x', '1')
    res = toDictionary(env.cmd('TFUNCTION', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['keyspace_triggers'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnAsyncNotificationConsumer(env):
    '''#!js api_version=1.0 name=foo
redis.registerKeySpaceTrigger("consumer", "", async function(c, data){
    return foo()
})
    '''
    env.cmd('set', 'x', '1')
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['keyspace_triggers'][0]['num_failed'])
    res = toDictionary(env.cmd('TFUNCTION', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['keyspace_triggers'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnSyncNotificationConsumerThatMoveAsync(env):
    '''#!js api_version=1.0 name=foo
redis.registerKeySpaceTrigger("consumer", "", function(c, data){
    return c.executeAsync(async() => {
        return foo();
    });
})
    '''
    env.cmd('set', 'x', '1')
    runUntil(env, 1, lambda: toDictionary(env.cmd('TFUNCTION', 'LIST', 'vvv'), 6)[0]['keyspace_triggers'][0]['num_failed'])
    res = toDictionary(env.cmd('TFUNCTION', 'list', 'vvv'), 6)
    env.assertContains(':4:', res[0]['keyspace_triggers'][0]['last_error']) # error on line 4

