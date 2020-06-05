import sys
import os
from RLTest import Env
import yaml
import time
from common import TimeLimit

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../deps/readies"))
import paella

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn


class testBasic:
    def __init__(self):
        self.env = Env()
        conn = getConnectionByEnv(self.env)
        for i in range(100):
            conn.execute_command('set', str(i), str(i))

    def testShardsGB(self):
        self.env.expect('rg.pyexecute', "GB('ShardsIDReader')."
                                        "map(lambda x:int(execute('dbsize')))."
                                        "aggregate(0, lambda r, x: x, lambda r, x:r + x).run()").contains(['100'])

    def testKeysOnlyGB(self):
        self.env.expect('rg.pyexecute', "GearsBuilder('KeysOnlyReader')."
                                        "map(lambda x:int(execute('get', x)))."
                                        "aggregate(0, lambda r, x: r + x, lambda r, x:r + x).run()").contains(['4950'])

    def testAvg(self):
        self.env.expect('rg.pyexecute', "GearsBuilder()."
                                        "map(lambda x:int(x['value']))."
                                        "avg().run()").contains(['49.5'])

    def testAggregate(self):
        self.env.expect('rg.pyexecute', "GearsBuilder()."
                                        "map(lambda x:int(x['value']))."
                                        "aggregate(0, lambda r, x: x + r, lambda r, x: x + r).run()").contains(['4950'])

    def testCount(self):
        self.env.expect('rg.pyexecute', "GearsBuilder().count().run()").contains(['100'])

    def testSort(self):
        self.env.expect('rg.pyexecute', "GearsBuilder().map(lambda x:int(x['value'])).sort().run()").contains([str(i) for i in range(100)])

    def testCountBy(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                           'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                           'countby(lambda x: x["value"]).collect().run()')
        a = []
        for r in res[0]:
            a.append(eval(r))
        self.env.assertContains({'key': '100', 'value': 50}, a)
        self.env.assertContains({'value': 50, 'key': '0'}, a)

    def testLocalAggregate(self):
        self.env.skipOnCluster()
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                           'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                           '__localAggregateby__(lambda x:x["value"], 0, lambda k, a, x: 1 + a).'
                                           'map(lambda x:(x["key"], x["value"])).run()')
        a = []
        for r in res[0]:
            a.append(eval(r))
        self.env.assertContains(('100', 50), a)
        self.env.assertContains(('0', 50), a)

    def testBasicQuery(self):
        id = self.env.cmd('rg.pyexecute', "GearsBuilder().map(lambda x:str(x)).collect().run()", 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[0]]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'type': 'string', 'event': 'None', 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicFilterQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().filter(lambda x: int(x["value"]) >= 50).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[0]]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'type': 'string', 'event': 'None', 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicMapQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().map(lambda x: x["value"]).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[0]]
        self.env.assertEqual(set(res), set([i for i in range(100)]))
        self.env.cmd('rg.dropexecution', id)

    def testBasicGroupByQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                          'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                          'groupby(lambda x: str(x["value"]), lambda key, a, vals: 1 + (a if a else 0)).'
                                          'map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        self.env.assertContains("{'key': '100', 'value': 50}", res[0])
        self.env.assertContains("{'key': '0', 'value': 50}", res[0])
        self.env.cmd('rg.dropexecution', id)

    def testBasicAccumulate(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                          'map(lambda x: int(x["value"])).'
                                          'accumulate(lambda a,x: x + (a if a else 0)).'
                                          'collect().'
                                          'accumulate(lambda a,x: x + (a if a else 0)).'
                                          'map(lambda x:str(x)).run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)[0]
        self.env.assertEqual(sum([a for a in range(100)]), int(res[0]))
        self.env.cmd('rg.dropexecution', id)

def testBytes(env):
    conn = getConnectionByEnv(env)
    conn.set("x", 1)
    conn.execute_command('rg.pyexecute', 'GB().repartition(lambda x: "y").foreach(lambda x: execute("set", "y", bytes([1,2,3]))).run("x")')
    env.assertTrue(conn.exists("y"))


def testBytearray(env):
    conn = getConnectionByEnv(env)
    conn.set("x", 1)
    conn.execute_command('rg.pyexecute', 'GB().repartition(lambda x: "y").foreach(lambda x: execute("set", "y", bytearray([1,2,3]))).run("x")')
    env.assertTrue(conn.exists("y"))


def testKeysOnlyReader(env):
    conn = getConnectionByEnv(env)

    conn.execute_command('set', 'xx', '1')
    conn.execute_command('set', 'xy', '1')
    conn.execute_command('set', 'y', '1')

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run()')[0]
    env.assertEqual(set(res), set(['xx', 'xy', 'y']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader", defaultArg="*").run()')[0]
    env.assertEqual(set(res), set(['xx', 'xy', 'y']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader", defaultArg="x*").run()')[0]
    env.assertEqual(set(res), set(['xx', 'xy']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader", defaultArg="xx*").run()')[0]
    env.assertEqual(set(res), set(['xx']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader", defaultArg="xx").run()')[0]
    env.assertEqual(set(res), set(['xx']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run("*")')[0]
    env.assertEqual(set(res), set(['xx', 'xy', 'y']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run("x*")')[0]
    env.assertEqual(set(res), set(['xx', 'xy']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run("xx*")')[0]
    env.assertEqual(set(res), set(['xx']))

    res = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run("xx")')[0]
    env.assertEqual(set(res), set(['xx']))


def testFlatMap(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "flatmap(lambda x: x['value'])."
                                 "collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(set(res[0]), set(['1', '2', '3']))
    env.cmd('rg.dropexecution', id)


def testLimit(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "flatmap(lambda x: x['value'])."
                                 "limit(1).collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(len(res[0]), 1)
    env.cmd('rg.dropexecution', id)


def testLimitWithOffset(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3', '4', '5')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "flatmap(lambda x: x['value'])."
                                 "limit(1, 3).collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(res[0], ['4'])
    env.cmd('rg.dropexecution', id)


def testRepartitionAndWriteOption(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "repartition(lambda x: x['value'])."
                                 "foreach(lambda x: redisgears.executeCommand('set', x['value'], x['key']))."
                                 "map(lambda x : str(x)).collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)[0]
    env.assertContains("'value': '1'", str(res))
    env.assertContains("'key': 'x'", str(res))
    env.assertContains("'value': '2'", str(res))
    env.assertContains("'key': 'y'", str(res))
    env.assertContains("'value': '3'", str(res))
    env.assertContains("'key': 'z'", str(res))
    env.assertEqual(conn.execute_command('get', '1'), 'x')
    env.assertEqual(conn.execute_command('get', '2'), 'y')
    env.assertEqual(conn.execute_command('get', '3'), 'z')
    env.cmd('rg.dropexecution', id)


def testBasicWithRun(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('xadd', 'stream', '*', 'test', '1')
    res = env.cmd('rg.pyexecute', "GearsBuilder('StreamReader')."
                                  "run('stream')")
    env.assertEqual(len(res[0]), 1)
    res = eval(res[0][0])
    env.assertEqual(res['value']['test'], '1')

def testTimeEvent(env):
    conn = getConnectionByEnv(env)
    conn.set('x', '1')
    conn.set('y', '1')
    conn.set('z', '1')
    script = '''
def func(x):
    var = int(redisgears.executeCommand('get',x['key'])) + 1
    redisgears.executeCommand('set',x['key'], var)
def OnTime():
    GearsBuilder().foreach(func).collect().run()
def start():
    redisgears.registerTimeEvent(2, OnTime)
start()
    '''
    env.cmd('rg.pyexecute', script)
    env.assertEqual(int(conn.get('x')), 1)
    env.assertEqual(int(conn.get('y')), 1)
    env.assertEqual(int(conn.get('z')), 1)
    time.sleep(3)
    env.assertTrue(int(conn.get('x')) >= 2)
    env.assertTrue(int(conn.get('y')) >= 2)
    env.assertTrue(int(conn.get('z')) >= 2)


def testTimeEventSurrviveRestart(env):
    env.skipOnCluster()
    env.execute_command('set', 'x', '1')
    script = '''
def func():
    oldVal = int(redisgears.executeCommand('get','x'))
    redisgears.executeCommand('set','x', str(oldVal + 1))
redisgears.registerTimeEvent(1, func, 'timeEvent')
    '''
    env.expect('rg.pyexecute', script).ok()
    for _ in env.reloading_iterator():
        res = env.cmd('keys', '*')
        env.assertEqual(set(res), set(['x', 'timeEvent']))
        res1 = env.cmd('get', 'x')
        time.sleep(2)
        res2 = env.cmd('get', 'x')
        env.assertTrue(int(res2) >= int(res1) + 1)

    env.expect('del', 'timeEvent').equal(1)
    res = env.cmd('keys', '*')
    env.assertEqual(set(res), set(['x']))
    res1 = env.cmd('get', 'x')
    time.sleep(1)
    res2 = env.cmd('get', 'x')
    env.assertTrue(int(res2) >= int(res1))


def testExecuteCommandWithNullTerminated(env):
    env.skipOnCluster()
    env.expect('set', 'x', 'test\x00test').equal(True)
    env.expect('get', 'x').equal('test\x00test')
    env.cmd('RG.PYEXECUTE', "GearsBuilder().foreach(lambda x: redisgears.executeCommand('SET', 'bar', str(x['value']))).map(lambda x: str(x)).run()")
    env.expect('get', 'bar').equal('test\x00test')


def testKeyWithUnparsedValue(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('sadd', 'x', '1', '2', '3')
    env.expect('RG.PYEXECUTE', "GB().map(lambda x: execute('smembers', x['key'])).flatmap(lambda x:x).sort().run()").contains(['1', '2', '3'])

def testMaxExecutions():
    ## todo: currently there is a problem with MaxExecutions which might cause running executions to be drop and the redis server
    ##       to crash, this is why I increased the sleep interval, we should fix it ASAP.
    env = Env(moduleArgs="MaxExecutions 3")
    conn = getConnectionByEnv(env)
    conn.execute_command('RG.PYEXECUTE', "GearsBuilder().map(lambda x: str(x)).register('*')", 'UNBLOCKING')
    time.sleep(1)
    conn.execute_command('set', 'x', '0')
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'x', '2')
    time.sleep(2)
    res = env.execute_command('RG.DUMPEXECUTIONS')
    # res is a list of the form ['executionId', '0000000000000000000000000000000000000000-0', 'status', 'done']
    env.assertTrue(len(res) == 3)
    env.assertTrue(set(map(lambda x: int(x[1].split('-')[1]), res)) == set([0, 1, 2]))
    conn.execute_command('set', 'x', '3')
    time.sleep(1)
    res = env.execute_command('RG.DUMPEXECUTIONS')
    env.assertTrue(set(map(lambda x: int(x[1].split('-')[1]), res)) == set([1, 2, 3]))
    map(lambda x: env.cmd('rg.dropexecution', x[1]), res)

    # delete all registrations so valgrind check will pass
    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

def testOneKeyScan(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    p = conn.pipeline()
    for i in range(2000):
        p.execute_command('set', "pref-%s" % i, "pref-%s" % i)
    p.execute()

    conn.execute_command('set', 'x', '1')
    env.expect('rg.pyexecute', "GB().count().run('pref*')").contains(['2000'])
    env.expect('rg.pyexecute', "GB().count().run('x*')").contains(['1'])

def testGlobalsSharedBetweenFunctions(env):
    conn = getConnectionByEnv(env)
    script = '''
counter = 0

def func1(r):
    global counter
    counter += 1
    return counter

def func2(a):
    global counter
    counter += 1
    return counter

GB('ShardsIDReader').map(func1).map(func2).collect().distinct().run()
    '''
    env.expect('rg.pyexecute', script).equal([['2'], []])
    env.expect('rg.pyexecute', script).equal([['2'], []])

def testSubinterpreterIsolation(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    script = '''
if 'x' not in globals().keys():
    x = 1

def returnX(a):
    global x
    x += 1
    return x

GB().map(returnX).run()
    '''
    env.expect('rg.pyexecute', script).equal([['2'], []])
    env.expect('rg.pyexecute', script).equal([['2'], []])

def testAbortExecution():
    env = Env(moduleArgs='executionThreads 1')
    env.skipOnCluster()
    infinitScript = '''
def InfinitLoop(r):
    import time
    while True:
        time.sleep(0.1)
    return r
GB().map(InfinitLoop).run()
    '''
    env.cmd('set', 'x', '1')

    executionId1 = env.cmd('rg.pyexecute', infinitScript, 'unblocking')
    executionId2 = env.cmd('rg.pyexecute', infinitScript, 'unblocking')
    try:
        with TimeLimit(2):
            status = None
            while status != 'running':
                status = env.cmd('RG.GETEXECUTION', executionId1)[0][3][1]
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for execution to start running')

    status2 = env.cmd('RG.GETEXECUTION', executionId2)[0][3][1]
    env.assertEqual(status2, 'created')

    env.expect('rg.abortexecution', executionId2).ok()
    res = env.cmd('RG.GETEXECUTION', executionId2)
    env.assertEqual(res[0][3][1], 'aborted')

    env.expect('rg.abortexecution', executionId1).ok()
    res = env.cmd('RG.GETEXECUTION', executionId1)
    env.assertEqual(res[0][3][1], 'done')
    env.assertEqual(len(res[0][3][9]), 1) # number if error is one
    
    env.expect('rg.dropexecution', executionId1).ok()
    env.expect('rg.dropexecution', executionId2).ok()

def testTimeEventSubinterpreterIsolation(env):
    env.skipOnCluster()
    script1 = '''
if 'x' not in globals().keys():
    x = 1
def OnTime():
    global x
    x += 1
    execute('set', 'x', str(x))
def start():
    redisgears.registerTimeEvent(2, OnTime)
start()
    '''
    script2 = '''
if 'x' not in globals().keys():
    x = 1
def OnTime():
    global x
    x += 1
    execute('set', 'y', str(x))
def start():
    redisgears.registerTimeEvent(2, OnTime)
start()
    '''
    env.cmd('rg.pyexecute', script1)
    env.cmd('rg.pyexecute', script2)
    env.assertIsNone(env.cmd('get', 'x'))
    env.assertIsNone(env.cmd('get', 'y'))
    time.sleep(3)
    env.assertTrue(int(env.cmd('get', 'x')) < 3)
    env.assertTrue(int(env.cmd('get', 'y')) < 3)

class testConfig:
    def __init__(self):
        self.env = Env()

    def testMaxExecutions(self):
        max_exe = self.env.execute_command('RG.CONFIGGET', 'MaxExecutions')
        n = long(max_exe[0]) + 1
        self.env.expect('RG.CONFIGSET', 'MaxExecutions', n).equal(['OK'])
        self.env.expect('RG.CONFIGGET', 'MaxExecutions').equal([n])

    def testNotModifiableAtRuntime(self):
        pyhome = self.env.execute_command('RG.CONFIGGET', 'DependenciesSha256')
        res = self.env.execute_command('RG.CONFIGSET', 'DependenciesSha256', '/')
        self.env.assertTrue('(error)' in str(res[0]))
        pyhome = self.env.execute_command('RG.CONFIGGET', 'DependenciesSha256')
        self.env.expect('RG.CONFIGSET', 'MaxExecutions', 10).equal(['OK'])

    def testNonExisting(self):
        res = self.env.execute_command('RG.CONFIGGET', 'NoSuchConfig1')
        self.env.assertTrue('(error)' in str(res[0]))

    def testMultiple(self):
        res = self.env.execute_command('RG.CONFIGGET', 'NoSuchConfig', 'MaxExecutions')
        self.env.assertTrue(str(res[0]).startswith('(error)') and not str(res[1]).startswith('(error)'))
        res = self.env.execute_command('RG.CONFIGSET', 'NoSuchConfig', 1, 'MaxExecutions', 10)
        self.env.assertTrue(str(res[0]) == 'OK - value was saved in extra config dictionary')
        self.env.expect('RG.CONFIGGET', 'MaxExecutions').equal([10L])


class testGetExecution:
    def __init__(self):
        self.env = Env()
        conn = getConnectionByEnv(self.env)
        conn.execute_command('SET', 'k1', 'spark')
        conn.execute_command('SET', 'k2', 'star')
        conn.execute_command('SET', 'k3', 'lambda')


    def testGettingANonExistingExecutionIdShouldError(self):
        self.env.expect('RG.GETEXECUTION', 'NoSuchExecutionId').raiseError()


    def testGettingAnExecutionPlanShouldSucceed(self):
        id = self.env.cmd('RG.PYEXECUTE', "GB().map(lambda x: x['value']).run()", 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id)
        self.env.assertEqual(res[0][3][7], 3)       # results
        self.env.assertEqual(len(res[0][3][9]), 0)  # errors
        self.env.cmd('RG.DROPEXECUTION', id)

    def testProfileExecutionsShouldBeDisabledByDefault(self):
        res = self.env.cmd('RG.CONFIGGET', 'ProfileExecutions')
        self.env.assertEqual(res[0], 0)


    def testExecutionShouldNotContainStepsDurationsWhenProfilingIsDisabled(self):
        res = self.env.cmd('RG.CONFIGSET', 'ProfileExecutions', 0)
        self.env.assertOk(res[0])
        id = self.env.cmd('RG.PYEXECUTE', "GB().map(lambda x: x['value']).run()", 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id)
        steps = res[0][3][15]
        self.env.assertLessEqual(1, len(steps))
        sdursum = 0
        for _, stype, _, sdur, _, sname, _, sarg in steps:
            sdursum += sdur    
        self.env.assertEqual(sdursum, 0)
        self.env.cmd('RG.DROPEXECUTION', id)


    def testExecutionsShouldContainSomeStepsDurationsWhenProfilingIsEnabled(self):
        res = self.env.cmd('RG.CONFIGSET', 'ProfileExecutions', 1)
        self.env.assertOk(res[0])
        id = self.env.cmd('RG.PYEXECUTE', "GB().flatmap(lambda x: x['value']).run()", 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.CONFIGSET', 'ProfileExecutions', 0)  # TODO: consider running the basicTests class with profiling
        self.env.assertOk(res[0])
        res = self.env.cmd('RG.GETEXECUTION', id)
        steps = res[0][3][15]
        self.env.assertLessEqual(1, len(steps))
        sdursum = 0
        for _, stype, _, sdur, _, sname, _, sarg in steps:
            sdursum += sdur    
        self.env.assertLessEqual(0, sdursum)
        self.env.cmd('RG.DROPEXECUTION', id)

    def testGetShardExecutionShouldSucceed(self):
        id = self.env.cmd('RG.PYEXECUTE', "GB().filter(lambda x: true).run()", 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id, 'sHARD')
        self.env.assertEqual(1, len(res))
        self.env.cmd('RG.DROPEXECUTION', id)

    def testGetClusterExecutionShouldSucceedWhenInClusterMode(self):
        if self.env.shardsCount < 2:  # TODO: RedisGears_IsClusterMode reports false for clusters with 1 shard
            self.env.skip()
        id = self.env.cmd('RG.PYEXECUTE', "GB().map(lambda x: x).run()", 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id, 'Cluster')
        self.env.assertLessEqual(1, len(res))
        self.env.cmd('RG.DROPEXECUTION', id)

def testConfigGet():
    env = Env(moduleArgs='TestConfig TestVal')
    env.skipOnCluster()
    env.expect('RG.PYEXECUTE', "GB('ShardsIDReader')."
                               "map(lambda x: (GearsConfigGet('TestConfig'), GearsConfigGet('NotExists', 'default')))."
                               "collect().distinct().run()").equal([["('TestVal', 'default')"],[]])

def testRecordSerializationFailure():
    env = Env(moduleArgs='CreateVenv 1')
    if env.shardsCount < 2:  # TODO: RedisGears_IsClusterMode reports false for clusters with 1 shard
        env.skip()
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                  "map(lambda x: __import__('redisgraph'))."
                  "collect().distinct().run()", 'REQUIREMENTS', 'redisgraph')
    env.assertEqual(len(res[1]), env.shardsCount - 1) # the initiator will not raise error

def testAtomic(env):
    conn = getConnectionByEnv(env)
    script = '''
def test(r):
    with atomic():
        execute('set', 'x{%s}' % hashtag(), 2)
        execute('set', 'y{%s}' % hashtag(), 1)
GB('ShardsIDReader').foreach(test).flatmap(lambda r: execute('mget', 'x{%s}' % hashtag(), 'y{%s}' % hashtag())).collect().distinct().sort().run()
    '''
    env.expect('RG.PYEXECUTE', script).equal([['1', '2'],[]])

def testParallelExecutions(env):
    conn = getConnectionByEnv(env)
    infinitScript = '''
import time
def InifinitLoop(r):
    while True:
        time.sleep(1)
GB('ShardsIDReader').foreach(InifinitLoop).run()
'''
    executionId = env.cmd('RG.PYEXECUTE', infinitScript, 'unblocking')
    env.expect('RG.PYEXECUTE', "GB('ShardsIDReader').count().run()").equal([[str(env.shardsCount)],[]])

    # we need to abort the execution with a gear to abort it on all the shards
    env.expect('RG.ABORTEXECUTION', executionId).ok()
    env.expect('RG.DROPEXECUTION', executionId).ok()

def testMaxIdle():
    env = Env(moduleArgs='ExecutionMaxIdleTime 500')
    if env.shardsCount == 1:
        env.skip()

    conn = getConnectionByEnv(env)

    longExecution = '''
import time

myHashTag = hashtag()

def Loop(r):
    if hashtag() != myHashTag:
        # wait for 1 second only if I am not the initiator
        time.sleep(1)
    return 1
GB('ShardsIDReader').map(Loop).run()
'''
    env.expect('RG.PYEXECUTE', longExecution).equal([['1'], ['Execution max idle reached']])

def testStreamReaderFromId(env):
    conn = getConnectionByEnv(env)

    id1 = conn.execute_command('XADD', 's', '*', 'foo', 'bar', 'foo1', 'bar1')
    id2 = conn.execute_command('XADD', 's', '*', 'foo2', 'bar2', 'foo3', 'bar3')

    res = env.cmd('RG.PYEXECUTE', "GB('StreamReader').run('s')")

    res = [eval(r) for r in res[0]]

    env.assertEqual([r['value'] for r in res], [{'foo': 'bar', 'foo1': 'bar1'}, {'foo2': 'bar2', 'foo3': 'bar3'}])

    res = env.cmd('RG.PYEXECUTE', "GB('StreamReader').run('s', fromId='%s')" % id1)

    res = [eval(r) for r in res[0]]

    env.assertEqual([r['value'] for r in res], [{'foo2': 'bar2', 'foo3': 'bar3'}])

def testKeysReaderKeyType(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'r', '1')
    conn.execute_command('lpush', 'l', '1', '2', '3')
    conn.execute_command('sadd', 's', '1', '2', '3')
    conn.execute_command('hset', 'h', 'key', 'val')
    conn.execute_command('zadd', 'z', '1', 'm')

    env.expect('RG.PYEXECUTE', 'GB().map(lambda x: x["type"]).sort().run()').equal([['hash', 'list', 'set', 'string', 'zset'],[]])

def testKeysReaderNoScan(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB().map(lambda x: x['type']).distinct().run('test', noScan=True)").equal([['empty'],[]])

def testKeysReaderDontReadValue(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    res = env.cmd('RG.PYEXECUTE', "GB().run(readValue=False)")
    res = [eval(r) for r in res[0]]
    env.assertEqual(res, [{'key':'x', 'event': None}])

def testKeysOnlyReader(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'z', '1')
    conn.execute_command('set', 'y', '1')
    env.expect('RG.PYEXECUTE', "GB('KeysOnlyReader').sort().run()").equal([['x', 'y', 'z'],[]])

def testKeysOnlyReaderWithPattern(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'z', '1')
    conn.execute_command('set', 'y', '1')
    env.expect('RG.PYEXECUTE', "GB('KeysOnlyReader').sort().run('x')").equal([['x'],[]])

def testKeysOnlyReaderWithCount(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'z', '1')
    conn.execute_command('set', 'y', '1')
    env.expect('RG.PYEXECUTE', "GB('KeysOnlyReader').sort().run(count=1)").equal([['x', 'y', 'z'],[]])

def testKeysOnlyReaderWithNoScan(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'x1', '1')
    conn.execute_command('set', 'x2', '1')
    env.expect('RG.PYEXECUTE', "GB('KeysOnlyReader').run('x', noScan=True)").equal([['x'],[]])

def testKeysOnlyReaderWithPatternGenerator(env):
    conn = getConnectionByEnv(env)
    script = '''
def Generator():
    return ('x{%s}' % hashtag(), True)
GB('KeysOnlyReader').count().run(patternGenerator=Generator)
    '''
    env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').foreach(lambda x: execute('set', 'x{%s}' % hashtag(), '1')).run()")
    env.expect('RG.PYEXECUTE', script).equal([[str(env.shardsCount)],[]])

def testWithMultiExec(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    conn.execute_command('multi')
    conn.execute_command('rg.pyexecute', 'GB().run()')
    try:
        conn.execute_command('exec')
        env.assertTrue(True, message='Did not get error when running gear in multi exec')
    except Exception:
        env.assertTrue(True, message='Got error when running gear in multi exec')
