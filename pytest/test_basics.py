from RLTest import Env
import yaml
import time


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
        res = [yaml.full_load(r) for r in res[0]]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicFilterQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().filter(lambda x: int(x["value"]) >= 50).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.full_load(r) for r in res[0]]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicMapQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().map(lambda x: x["value"]).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.full_load(r) for r in res[0]]
        self.env.assertEqual(set(res), set([i for i in range(100)]))
        self.env.cmd('rg.dropexecution', id)

    def testBasicGroupByQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                          'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                          'groupby(lambda x: str(x["value"]), lambda key, a, vals: 1 + (a if a else 0)).'
                                          'map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        self.env.assertContains("{'value': 50, 'key': '100'}", res[0])
        self.env.assertContains("{'value': 50, 'key': '0'}", res[0])
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
    env.assertEqual(res['test'], '1')


def testBasicStream(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "GearsBuilder()."
                                  "filter(lambda x:x['key'] != 'values')."
                                  "repartition(lambda x: 'values')."
                                  "foreach(lambda x: redisgears.executeCommand('lpush', 'values', x['value']))."
                                  "register('*')", 'UNBLOCKING')
    env.assertEqual(res, 'OK')
    if(res != 'OK'):
        return
    time.sleep(0.5)  # make sure the execution reached to all shards
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    res = []
    while len(res) < 6:
        res = env.cmd('rg.dumpexecutions')
    for e in res:
        env.broadcast('rg.getresultsblocking', e[1])
        env.cmd('rg.dropexecution', e[1])
    env.assertEqual(set(conn.lrange('values', '0', '-1')), set(['1', '2', '3']))


def testBasicStreamRegisterOnPrefix(env):
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', "GearsBuilder('StreamReader')."
                               "map(lambda x: str(x))."
                               "repartition(lambda x: 'new_key')."
                               "foreach(lambda x: redisgears.executeCommand('set', 'new_key', x))."
                               "register('s*')").ok()
    conn.execute_command('xadd', 'stream1', '*', 'name', 'test')
    res = []
    while len(res) < 1:
        res = env.cmd('rg.dumpexecutions')
    env.broadcast('rg.getresultsblocking', res[0][1])
    env.cmd('rg.dropexecution', res[0][1])
    env.assertContains("{'name': 'test', 'streamId': ", conn.get('new_key'))

    conn.execute_command('xadd', 'stream2', '*', 'name', 'test1')
    res = []
    while len(res) < 1:
        res = env.cmd('rg.dumpexecutions')
    env.broadcast('rg.getresultsblocking', res[0][1])
    env.cmd('rg.dropexecution', res[0][1])
    env.assertContains("{'name': 'test1', 'streamId': ", conn.get('new_key'))

    conn.execute_command('xadd', 'rstream1', '*', 'name', 'test2')
    env.assertContains("{'name': 'test1', 'streamId': ", conn.get('new_key'))


def testBasicStreamProcessing(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "GearsBuilder('StreamReader')."
                                  "flatmap(lambda x: [(a[0], a[1]) for a in x.items()])."
                                  "repartition(lambda x: x[0])."
                                  "foreach(lambda x: redisgears.executeCommand('set', x[0], x[1]))."
                                  "map(lambda x: str(x))."
                                  "register('stream1')", 'UNBLOCKING')
    env.assertEqual(res, 'OK')
    if(res != 'OK'):
        return
    time.sleep(0.5)  # make sure the execution reached to all shards
    env.cmd('XADD', 'stream1', '*', 'f1', 'v1', 'f2', 'v2')
    res = []
    while len(res) < 1:
        res = env.cmd('rg.dumpexecutions')
    for e in res:
        env.broadcast('rg.getresultsblocking', e[1])
        env.cmd('rg.dropexecution', e[1])
    env.assertEqual(conn.get('f1'), 'v1')
    env.assertEqual(conn.get('f2'), 'v2')


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
    env.expect('set', 'x', 'test\x00test').equal('OK')
    env.expect('get', 'x').equal('test\x00test')
    env.cmd('RG.PYEXECUTE', "GearsBuilder().foreach(lambda x: redisgears.executeCommand('SET', 'bar', str(x['value']))).map(lambda x: str(x)).run()")
    env.expect('get', 'bar').equal('test\x00test')


def testKeyWithUnparsedValue(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('sadd', 'x', '1', '2', '3')
    env.expect('RG.PYEXECUTE', "GB().map(lambda x: execute('smembers', x['key'])).flatmap(lambda x:x).sort().run()").contains(['1', '2', '3'])

def testMaxExecutions():
    env = Env(moduleArgs="MaxExecutions 3")
    conn = getConnectionByEnv(env)
    conn.execute_command('RG.PYEXECUTE', "GearsBuilder().map(lambda x: str(x)).register('*')", 'UNBLOCKING')
    time.sleep(1)
    conn.execute_command('set', 'x', '0')
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'x', '2')
    time.sleep(1)
    res = env.execute_command('RG.DUMPEXECUTIONS')
    # res is a list of the form ['executionId', '0000000000000000000000000000000000000000-0', 'status', 'done']
    env.assertTrue(len(res) == 3)
    env.assertTrue(map(lambda x: int(x[1].split('-')[1]), res) == [0, 1, 2])
    conn.execute_command('set', 'x', '3')
    time.sleep(1)
    res = env.execute_command('RG.DUMPEXECUTIONS')
    env.assertTrue(map(lambda x: int(x[1].split('-')[1]), res) == [1, 2, 3])
    map(lambda x: env.cmd('rg.dropexecution', x[1]), res)

def testOneKeyScan(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    p = conn.pipeline()
    for i in range(200000):
        p.execute_command('set', "pref-%s" % i, "pref-%s" % i)
    p.execute()

    conn.execute_command('set', 'x', '1')
    env.expect('rg.pyexecute', "GB().count().run('pref*')").contains(['200000'])
    env.expect('rg.pyexecute', "GB().count().run('x*')").contains(['1'])


class testConfig:
    def __init__(self):
        self.env = Env()

    def testMaxExecutions(self):
        max_exe = self.env.execute_command('RG.CONFIGGET', 'MaxExecutions')
        n = long(max_exe[0]) + 1
        self.env.expect('RG.CONFIGSET', 'MaxExecutions', n).equal(['OK'])
        self.env.expect('RG.CONFIGGET', 'MaxExecutions').equal([n])

    def testNotModifiableAtRuntime(self):
        pyhome = self.env.execute_command('RG.CONFIGGET', 'PythonHomeDir')
        res = self.env.execute_command('RG.CONFIGSET', 'PythonHomeDir', '/')
        self.env.assertTrue(res[0].startswith('(error)'))
        pyhome = self.env.execute_command('RG.CONFIGGET', 'PythonHomeDir')
        self.env.expect('RG.CONFIGSET', 'MaxExecutions', 10).equal(['OK'])

    def testNonExisting(self):
        res = self.env.execute_command('RG.CONFIGGET', 'NoSuchConfig')
        self.env.assertTrue(res[0].startswith('(error)'))
        res = self.env.execute_command('RG.CONFIGSET', 'NoSuchConfig', 1)
        self.env.assertTrue(res[0].startswith('(error)'))

    def testMultiple(self):
        res = self.env.execute_command('RG.CONFIGGET', 'NoSuchConfig', 'MaxExecutions')
        self.env.assertTrue(str(res[0]).startswith('(error)') and not str(res[1]).startswith('(error)'))
        res = self.env.execute_command('RG.CONFIGSET', 'NoSuchConfig', 1, 'MaxExecutions', 10)
        self.env.assertTrue(str(res[0]).startswith('(error)'))
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
        self.env.assertLess(0, sdursum)
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

