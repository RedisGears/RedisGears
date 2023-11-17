from RLTest import Env
import time
from includes import *
from common import gearsTest

class testGenericErrors:
    def __init__(self):
        self.env = Env()


    def testInvalidSyntax(self):
        self.env.expect('rg.pyexecute', '1defs + GearsBuilder().notexists()').error().contains("invalid syntax")


    def testScriptError(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().notexists()').error().contains("notexists")


    def testBuilderCreationWithUnexistingReader(self):
        self.env.expect('rg.pyexecute', 'GB("unexists").accumulate(lambda a, x: 1 + (a if a else 0)).run()').error().contains('reader does not exists')

    def testTwoExecutionsInOneScript(self):
        script = '''
GB().run()
GB().run()
'''
        self.env.expect('rg.pyexecute', script).error().contains('more then 1')

    def testRegistrationFailureOnSerialization(self):
        script1 = '''
import redis
r = redis.Redis('localhost', '6381')

def test(x):
    r.set('x', '1')
    return x

GB().map(test).register()
'''
        script2 = '''
import redis
r = redis.Redis('localhost', '6381')

def test(x):
    r.set('x', '1')
    return x

GB('StreamReader').map(test).register()
'''
        self.env.expect('rg.pyexecute', script1, 'REQUIREMENTS', 'redis==3.5.3').error().contains('Error occured when serialized a python callback')
        self.env.expect('rg.pyexecute', script2, 'REQUIREMENTS', 'redis==3.5.3').error().contains('Error occured when serialized a python callback')

@gearsTest()
def testRunFailureOnSerialization(env):
    if env.shardsCount < 2:
        env.skip()
    conn = getConnectionByEnv(env)
    script1 = '''
import redis
r = redis.Redis('localhost', 6381) # set none existing port to avoid deadlock

def test(x):
    r.set('x', '1')
    return x

GB().map(test).run()
'''

    script2 = '''
import redis
r = redis.Redis('localhost', 6381) # set none existing port to avoid deadlock

def test(x):
    r.set('x', '1')
    return x

GB('StreamReader').map(test).run()
'''
    env.expect('rg.pyexecute', script1).error().contains('Error occured when serialized a python callback')
    env.expect('rg.pyexecute', script2).error().contains('Error occured when serialized a python callback')


class testStepsErrors:
    def __init__(self):
        self.env = Env(decodeResponses=True)
        self.conn = getConnectionByEnv(self.env)
        self.conn.execute_command('set', 'x', '1')
        self.conn.execute_command('set', 'y', '1')

    def testForEachError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().foreach(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testGroupByError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().groupby(lambda x: "str", lambda a, x, k: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testBatchGroupByError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().batchgroupby(lambda x: "str", lambda x, k: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testExtractorError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().groupby(lambda x: notexists(x), lambda a, x, k: 1).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testAccumulateError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().accumulate(lambda a, x: notexists(a, x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))

    def testAccumulateError2(self):
        script = '''
callNum = 0
def secondCallFailed(a, x):
    global callNum
    if callNum > 0:
        raise Exception('failed')
    callNum+=1
    return x
GearsBuilder().accumulate(secondCallFailed).collect().run()
        '''
        res = self.env.cmd('rg.pyexecute', script)
        self.env.assertLessEqual(1, len(res[1]))

    def testAccumulatebyError(self):
        script = '''
callNum = 0
def secondCallFailed(k, a, x):
    global callNum
    if callNum > 0:
        raise Exception('failed')
    callNum+=1
    return x
GearsBuilder().aggregateby(lambda x: 'test', 0, secondCallFailed, secondCallFailed).collect().run()
        '''
        res = self.env.cmd('rg.pyexecute', script)
        self.env.assertLessEqual(1, len(res[1]))

    def testAggregateByError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().aggregateby(lambda x: "1",{},lambda k, a, x: (x["kaka"] if x["key"]=="y" else x), lambda k, a, x: (x["kaka"] if x["key"]=="y" else x)).run()')
        self.env.assertLessEqual(1, len(res[1]))

    def testMapError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().map(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testFlatMapError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().flatmap(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testFilterError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().filter(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))


    def testRepartitionError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().repartition(lambda x: notexists(x)).repartition(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, len(res[1]))

@gearsTest()
def testCommandReaderWithRun(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").run()').error().contains('reader do not support run')

@gearsTest()
def testCommandReaderWithBadArgs(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register("test")').error().contains('no trigger or hook argument was given')
    env.expect('rg.pyexecute', 'GB("CommandReader").register(trigger=1)').error().contains('trigger argument is not string')

@gearsTest()
def testCommandReaderRegisterSameCommand(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register(trigger="command")').ok()
    env.expect('rg.pyexecute', 'GB("CommandReader").register(trigger="command")').error().contains('trigger already registered')

@gearsTest()
def testCommandReaderRegisterWithExcpetionCommand(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").foreach(lambda x: noexists).register(trigger="command")').ok()
    env.expect('rg.trigger', 'command').error().contains("'noexists' is not defined")

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'})
def testNoSerializableRegistrationWithAllReaders(env):
    script = '''
import redis
r = redis.Redis('localhost', 6381)
GB('%s').map(lambda x: r).register(trigger='test')
    '''
    env.expect('RG.PYEXECUTE', script % 'KeysReader', 'REQUIREMENTS', 'redis==3.5.3').error()
    env.expect('RG.PYEXECUTE', script % 'StreamReader', 'REQUIREMENTS', 'redis==3.5.3').error()
    env.expect('RG.PYEXECUTE', script % 'CommandReader', 'REQUIREMENTS', 'redis==3.5.3').error()

@gearsTest()
def testExtraUnknownArgumentsReturnError(env):
    env.expect('RG.PYEXECUTE', 'GB().run()', 'exta', 'unknown', 'arguments').error()

class testStepsWrongArgs:
    def __init__(self):
        self.env = Env(decodeResponses=True)
        self.conn = getConnectionByEnv(self.env)

    def testRegisterWithWrongRegexType(self):
        self.env.expect('rg.pyexecute', 'GB().register(1)').error().contains('regex argument must be a string')

    def testRegisterWithWrongEventKeysTypesList(self):
        self.env.expect('rg.pyexecute', 'GB().register(regex="*", eventTypes=1)').error().contains('not iterable')
        self.env.expect('rg.pyexecute', 'GB().register(regex="*", keyTypes=1)').error().contains('not iterable')
        self.env.expect('rg.pyexecute', 'GB().register(regex="*", eventTypes=[1, 2, 3])').error().contains('type is not string')
        self.env.expect('rg.pyexecute', 'GB().register(regex="*", keyTypes=[1, 2, 3])').error().contains('type is not string')

    def testRegisterWithWrongCommandsParam(self):
        self.env.expect('rg.pyexecute', 'GB().register(regex="*", commands=1)').error().contains('not iterable')
        self.env.expect('rg.pyexecute', 'GB().register(regex="*", commands=[1,2,3])').error().contains('type is not string')

    def testGearsBuilderWithWrongBuilderArgType(self):
        self.env.expect('rg.pyexecute', 'GB(1).run()').error().contains('reader argument must be a string')

    def testExecuteWithWrongCommandArgType(self):
        self.env.expect('rg.pyexecute', 'execute(1)').error().contains('the given command must be a string')

    def testTimeEventWithWrongCallbackArg(self):
        self.env.expect('rg.pyexecute', 'registerTE(2, 2)').error().contains('callback must be a function')

    def testTimeEventWithWrongTimeArg(self):
        self.env.expect('rg.pyexecute', 'registerTE("2", lambda x: str(x))').error().contains('time argument must be a long')

    def testMapWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().map(1, 2).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().map(1).run()').error().contains('argument must be a function')


    def testFilterWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().filter(1, 2).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().filter(1).run()').error().contains('argument must be a function')


    def testGroupByWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().groupby(1, 2, 3).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().groupby(1, 2).run()').error().contains('argument must be a function')


    def testBatchGroupByWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().batchgroupby(1, 2, 3).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().batchgroupby(1, 2).run()').error().contains('argument must be a function')


    def testCollectWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().collect(1, 2, 3).run()').error().contains('wrong number of args')


    def testForEachWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().foreach(1, 2).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().foreach(1).run()').error().contains('argument must be a function')


    def testRepartitionWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().repartition(1, 2).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().repartition(1).run()').error().contains('argument must be a function')


    def testLimitWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().limit().run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().limit(1, 2, 3).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().limit("awdwada").run()').error().contains('argument must be a number')
        self.env.expect('rg.pyexecute', 'GB().limit(1, "kakaka").run()').error().contains('argument must be a number')


    def testAccumulateWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().accumulate(1, 2).run()').error().contains('wrong number of args')
        self.env.expect('rg.pyexecute', 'GB().accumulate(1).run()').error().contains('argument must be a function')


    def testAvgWrongArgs(self):
        self.env.expect('rg.pyexecute', 'GB().avg(1).run()').error().contains('argument must be a function')


    def testPyReaderWithWrongArgument(self):
        self.env.expect('rg.pyexecute', 'GB("PythonReader").run("*")').error().contains('pyreader argument must be a functio')
        self.env.expect('rg.pyexecute', 'GB("PythonReader").run()').error().contains('pyreader argument must be a functio')
        self.env.expect('rg.pyexecute', 'GB("PythonReader", "*").run()').error().contains('pyreader argument must be a functio')
        self.env.expect('rg.pyexecute', 'GB("PythonReader", shardReaderCallback).run("*")').error().contains('pyreader argument must be a functio')

    def testStreamReaderBadFromIdFormat(self):
        self.conn.execute_command('XADD', 's', '*', 'foo', 'bar', 'foo1', 'bar1')
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").run("s", fromId="test")').equal([[], ['ERR Invalid stream ID specified as stream command argument']])

    def testStreamReaderBadFromId(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").run("s", fromId=1)').error()

    def testKeysReaderNoScanBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().run(noScan=1)').error()

    def testKeysReaderReadValueBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().run(readValue=1)').error()

    def testOnRegisteredBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().register(onRegistered=1)').error()

    def testRegisterModeBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().register(mode=1)').error()
        self.env.expect('rg.pyexecute', 'GearsBuilder().register(mode="test")').error()

    def testRegisterPrefixBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().register(prefix=1)').error()

    def testStreamReaderBatchBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(batch="test")').error()

    def testStreamReaderBatchBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(batch="test")').error()

    def testStreamReaderDurationBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(duration="test")').error()

    def testStreamReaderOnFailedPolicyBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(onFailedPolicy="test")').error()
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(onFailedPolicy=1)').error()

    def testStreamReaderOnFailedRetryIntervalBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(onFailedRetryInterval="test")').error()

    def testStreamReaderTrimStreamBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder("StreamReader").register(trimStream="test")').error()
        
    def testKeysReadeReadValueBadValue(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().register(readValue=1)').error()

    def testKeysOnlyReadeBadCount(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder("KeysOnlyReader").run(count="noNunber")')
        self.env.assertContains('value is not an integer', res[1][0])

    def testKeysOnlyReadeBadPatternGenerator(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder("KeysOnlyReader").run(patternGenerator="adwaw")')
        self.env.assertContains('object is not callable', res[1][0])


class testGetExecutionErrorReporting:
    def __init__(self):
        self.env = Env(decodeResponses=True)
        conn = getConnectionByEnv(self.env)
        conn.execute_command('set', '0', 'falsE')
        conn.execute_command('set', '1', 'truE')
        conn.execute_command('set', '', 'mebbE')


    def testErrorShouldBeReportedWithTracebackAttempted(self):
        self.env.cmd('RG.CONFIGSET', 'PythonAttemptTraceback', 1)
        id = self.env.cmd('RG.PYEXECUTE', 'GearsBuilder().repartition(lambda x: notexists(x)).repartition(lambda x: notexists(x)).collect().run()', 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id)
        errors = res[0][3][9]
        for error in errors:
            self.env.assertContains("name \'notexists\' is not defined", error)
        self.env.cmd('RG.DROPEXECUTION', id)


    def testErrorShouldBeReportedWithTracebackNotAttempted(self):
        self.env.cmd('RG.CONFIGSET', 'PythonAttemptTraceback', 0)
        id = self.env.cmd('RG.PYEXECUTE', 'GearsBuilder().repartition(lambda x: notexists(x)).repartition(lambda x: notexists(x)).collect().run()', 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id)
        errors = res[0][3][9]
        for error in errors:
            self.env.assertContains("name 'notexists' is not defined", error)
        self.env.cmd('RG.DROPEXECUTION', id)
        self.env.cmd('RG.CONFIGSET', 'PythonAttemptTraceback', 1)

@gearsTest()
def testCommandReaderWithPrefixRaiseError(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register(hook="test", keyprefix="foo")').error().contains('Can not override an unexisting command')

@gearsTest()
def testCommandReaderOverrideCommandWithMovableKeysRaiseError(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register(hook="xread", keyprefix="foo")').error().contains('Can not override a command with moveable keys by key prefix')

@gearsTest()
def testCommandReaderOverrideMultiRaiseError(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register(hook="multi")').error().contains('Can not override a command which are not allowed inside a script')
    
@gearsTest()
def testCommandReaderOverrideExecRaiseError(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register(hook="exec")').error().contains('Can not override a command which are not allowed inside a script')

@gearsTest()
def testCommandReaderOverrideBlpopRaiseError(env):
    env.expect('rg.pyexecute', 'GB("CommandReader").register(hook="blpop")').error().contains('Can not override a command which are not allowed inside a script')

@gearsTest()
def testKeysReaderWithUnexistingCommandRaiseError(env):
    env.expect('rg.pyexecute', 'GB().register(commands=["test"])').error().contains('bad reply')

@gearsTest()
def testKeysReaderOverrideCommandWithMovableKeysRaiseError(env):
    env.expect('rg.pyexecute', 'GB().register(commands=["xread"])').error().contains('Can not hook a command with moveable keys by key prefix')
    
@gearsTest()
def testKeysReaderOverrideMultiRaiseError(env):
    env.expect('rg.pyexecute', 'GB().register(commands=["multi"])').error().contains('Can not hook a command which are not allowed inside a script')

@gearsTest()    
def testKeysReaderOverrideExecRaiseError(env):
    env.expect('rg.pyexecute', 'GB().register(commands=["exec"])').error().contains('Can not hook a command which are not allowed inside a script')

@gearsTest()
def testKeysReaderOverrideBlpopRaiseError(env):
    env.expect('rg.pyexecute', 'GB().register(commands=["blpop"])').error().contains('Can not hook a command which are not allowed inside a script')

@gearsTest()
def testCallNextOnMapWrongScopes(env):
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').map(lambda r: call_next(r)).run()")
    env.assertIn('Can not get CommandHook ctx', res[1][0])

@gearsTest()
def testCallNextOnFilterWrongScopes(env):
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').filter(lambda r: call_next(r)).run()")
    env.assertIn('Can not get CommandHook ctx', res[1][0])

@gearsTest()
def testCallNextOnForeachWrongScopes(env):
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').foreach(lambda r: call_next(r)).run()")
    env.assertIn('Can not get CommandHook ctx', res[1][0])

@gearsTest()
def testCallNextOnFlatmapWrongScopes(env):
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').flatmap(lambda r: call_next(r)).run()")
    env.assertIn('Can not get CommandHook ctx', res[1][0])

@gearsTest()
def testCallNextOnAccumulateWrongScopes(env):
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').accumulate(lambda a,r: call_next(r)).run()")
    env.assertIn('Can not get CommandHook ctx', res[1][0])

@gearsTest()
def testCallNextOnAccumulatebyWrongScopes(env):
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').groupby(lambda x: x, lambda k,a,r: call_next(r)).run()")
    env.assertIn('Can not get CommandHook ctx', res[1][0])

@gearsTest()
def testCallNextOnWrongScopes(env):
    env.expect('rg.pyexecute', 'call_next()').error().contains('Can not get CommandHook ctx')

@gearsTest()
def testCallNextOnCoroWrongScope(env):
    script = '''
import asyncio

async def doTest(x):
    return call_next()

GB("ShardsIDReader").map(doTest).run()
    '''

    res = env.cmd('rg.pyexecute', script)
    err = res[1][0]
    env.assertContains(err, 'Can not get CommandHook ctx')

@gearsTest()
def testAwaitIsNotAllowedInsideAtomicBlock(env):
    script = '''
import asyncio

async def doTest(x):
    with atomic():
        return await asyncio.sleep(1)

GB("ShardsIDReader").map(doTest).run()
    '''

    res = env.cmd('rg.pyexecute', script)
    err = res[1][0]
    env.assertContains(err, 'await is not allow inside atomic block')

@gearsTest()
def testAwaitIsNotAllowedInsideMultiExecExecution(env):
    script = '''
import asyncio

async def doTest(x):
    await asyncio.sleep(1)
    return x

GB("CommandReader").map(doTest).register(trigger='test', mode='sync')
    '''

    env.cmd('rg.pyexecute', script)
    env.cmd('multi')
    env.cmd('rg.trigger', 'test')
    res = env.cmd('exec')
    env.assertContains('Creating async record is not allow', str(res[0]))

@gearsTest(skipOnCluster=True)
def testCommandOverrideWithMoreThenSingleResultReturnError(env):
    script = '''
GB("CommandReader").flatmap(lambda x: x).register(hook='hset', mode='sync')
    '''

    env.cmd('rg.pyexecute', script)
    env.expect('hset', 'h', 'foo', 'bar').error().contains('Command hook must return exactly one result')

@gearsTest()
def testCannotOverrideNoneSyncRegisration(env):
    script = '''
GB("CommandReader").flatmap(lambda x: x).register(hook='hset')
    '''

    env.expect('rg.pyexecute', script).equal('OK')
    env.expect('rg.pyexecute', script).error().contains('Can not override a none sync registration')
