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
        for r in res[1]:
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
        for r in res[1]:
            a.append(eval(r))
        self.env.assertContains(('100', 50), a)
        self.env.assertContains(('0', 50), a)

    def testBasicQuery(self):
        id = self.env.cmd('rg.pyexecute', "GearsBuilder().map(lambda x:str(x)).collect().run()", 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[1]]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicFilterQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().filter(lambda x: int(x["value"]) >= 50).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[1]]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicMapQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().map(lambda x: x["value"]).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[1]]
        self.env.assertEqual(set(res), set([i for i in range(100)]))
        self.env.cmd('rg.dropexecution', id)

    def testBasicGroupByQuery(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                          'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                          'groupby(lambda x: str(x["value"]), lambda key, a, vals: 1 + (a if a else 0)).'
                                          'map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        self.env.assertContains("{'value': 50, 'key': '100'}", res[1])
        self.env.assertContains("{'value': 50, 'key': '0'}", res[1])
        self.env.cmd('rg.dropexecution', id)

    def testBasicAccumulate(self):
        id = self.env.cmd('rg.pyexecute', 'GearsBuilder().'
                                          'map(lambda x: int(x["value"])).'
                                          'accumulate(lambda a,x: x + (a if a else 0)).'
                                          'collect().'
                                          'accumulate(lambda a,x: x + (a if a else 0)).'
                                          'map(lambda x:str(x)).run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)[1]
        self.env.assertEqual(sum([a for a in range(100)]), int(res[0]))
        self.env.cmd('rg.dropexecution', id)


def testFlatMap(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "flatmap(lambda x: x['value'])."
                                 "collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(set(res[1]), set(['1', '2', '3']))
    env.cmd('rg.dropexecution', id)


def testLimit(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "flatmap(lambda x: x['value'])."
                                 "limit(1).collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(len(res[1]), 1)
    env.cmd('rg.dropexecution', id)


def testLimitWithOffset(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3', '4', '5')
    id = env.cmd('rg.pyexecute', "GearsBuilder()."
                                 "flatmap(lambda x: x['value'])."
                                 "limit(1, 3).collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(res[1], ['4'])
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
    res = env.cmd('rg.getresultsblocking', id)[1]
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
    env.assertEqual(len(res[1]), 1)
    res = eval(res[1][0])
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
