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
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').error().contains('thread is already blocked')
    
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
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').error().contains('thread is not locked')
    
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
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test1').error().contains('is not allowed on script mode')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test2').error().contains('is not allowed on script mode')

@gearsTest()
def testJSStackOverflow(env):
    """#!js name=foo
function test() {
    test();
}
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').error().contains('Maximum call stack size exceeded')

@gearsTest()
def testJSStackOverflowOnLoading(env):
    script = """#!js name=foo
function test() {
    test();
}
test();
redis.register_function('test', test);
    """
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Maximum call stack size exceeded")
