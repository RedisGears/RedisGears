from common import gearsTest
from common import toDictionary
from common import runUntil

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnFunctionLoad(env):
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})
foo()
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains(':5:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnFunctionRun(env):
    '''#!js name=foo
redis.register_function("test", function(client){
    return foo()
})
    '''
    env.expect('RG.FCALL', 'foo', 'test', 0).error().contains(':3:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnAsyncFunctionRun(env):
    '''#!js name=foo
redis.register_function("test", async function(client){
    return foo()
})
    '''
    env.expect('RG.FCALL', 'foo', 'test', 0).error().contains(':3:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnFunctionThatReturnsCoro(env):
    '''#!js name=foo
redis.register_function("test", function(client){
    return client.run_on_background(async ()=>{
        return foo();
    });
})
    '''
    env.expect('RG.FCALL', 'foo', 'test', 0).error().contains(':4:') # error on line 5

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnStreamProcessing(env):
    '''#!js name=foo
redis.register_stream_consumer("consumer", "stream", 1, false, function(c, data){
    return foo()
})
    '''
    env.cmd('xadd', 'stream', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('rg.function', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['stream_consumers'][0]['streams'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnAsyncStreamProcessing(env):
    '''#!js name=foo
redis.register_stream_consumer("consumer", "stream", 1, false, async function(c, data){
    return foo()
})
    '''
    env.cmd('xadd', 'stream', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_consumers'][0]['streams'][0]['total_record_processed'])
    res = toDictionary(env.cmd('rg.function', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['stream_consumers'][0]['streams'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnNotificationConsumer(env):
    '''#!js name=foo
redis.register_notifications_consumer("consumer", "", function(c, data){
    return foo()
})
    '''
    env.cmd('set', 'x', '1')
    res = toDictionary(env.cmd('rg.function', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['notifications_consumers'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnAsyncNotificationConsumer(env):
    '''#!js name=foo
redis.register_notifications_consumer("consumer", "", async function(c, data){
    return foo()
})
    '''
    env.cmd('set', 'x', '1')
    runUntil(env, 1, lambda: toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['notifications_consumers'][0]['num_failed'])
    res = toDictionary(env.cmd('rg.function', 'list', 'vvv'), 6)
    env.assertContains(':3:', res[0]['notifications_consumers'][0]['last_error']) # error on line 3

@gearsTest(errorVerbosity=2)
def testVerboseErrorOnSyncNotificationConsumerThatMoveAsync(env):
    '''#!js name=foo
redis.register_notifications_consumer("consumer", "", function(c, data){
    return c.run_on_background(async() => {
        return foo();
    });  
})
    '''
    env.cmd('set', 'x', '1')
    runUntil(env, 1, lambda: toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['notifications_consumers'][0]['num_failed'])
    res = toDictionary(env.cmd('rg.function', 'list', 'vvv'), 6)
    env.assertContains(':4:', res[0]['notifications_consumers'][0]['last_error']) # error on line 4

