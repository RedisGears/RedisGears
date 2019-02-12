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

    def testBasicQuery(self):
        id = self.env.cmd('rg.pyexecute', "gearsCtx().map(lambda x:str(x)).collect().run()", 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[1]]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicFilterQuery(self):
        id = self.env.cmd('rg.pyexecute', 'gearsCtx().filter(lambda x: int(x["value"]) >= 50).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[1]]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rg.dropexecution', id)

    def testBasicMapQuery(self):
        id = self.env.cmd('rg.pyexecute', 'gearsCtx().map(lambda x: x["value"]).map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        res = [yaml.load(r) for r in res[1]]
        self.env.assertEqual(set(res), set([i for i in range(100)]))
        self.env.cmd('rg.dropexecution', id)

    def testBasicGroupByQuery(self):
        id = self.env.cmd('rg.pyexecute', 'gearsCtx().'
                                          'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                          'groupby(lambda x: str(x["value"]), lambda key, a, vals: 1 + (a if a else 0)).'
                                          'map(lambda x:str(x)).collect().run()', 'UNBLOCKING')
        res = self.env.cmd('rg.getresultsblocking', id)
        self.env.assertContains("{'value': 50, 'key': '100'}", res[1])
        self.env.assertContains("{'value': 50, 'key': '0'}", res[1])
        self.env.cmd('rg.dropexecution', id)

    def testBasicAccumulate(self):
        id = self.env.cmd('rg.pyexecute', 'gearsCtx().'
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
    id = env.cmd('rg.pyexecute', "gearsCtx()."
                                 "flatmap(lambda x: x['value'])."
                                 "collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(set(res[1]), set(['1', '2', '3']))
    env.cmd('rg.dropexecution', id)


def testLimit(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rg.pyexecute', "gearsCtx()."
                                 "flatmap(lambda x: x['value'])."
                                 "limit(1).collect().run()", 'UNBLOCKING')
    res = env.cmd('rg.getresultsblocking', id)
    env.assertEqual(len(res[1]), 1)
    env.cmd('rg.dropexecution', id)


def testRepartitionAndWriteOption(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    id = env.cmd('rg.pyexecute', "gearsCtx()."
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


def testBasicStream(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "gearsCtx()."
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
    while len(res) < 3:
        res = env.cmd('rg.dumpexecutions')
    for e in res:
        env.broadcast('rg.getresultsblocking', e[1])
        env.cmd('rg.dropexecution', e[1])
    env.assertEqual(set(conn.lrange('values', '0', '-1')), set(['1', '2', '3']))


def testBasicStreamRegisterOnPrefix(env):
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', "gearsCtx('StreamReader')."
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
    res = env.cmd('rg.pyexecute', "gearsCtx('StreamReader')."
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
def defineVar(x):
    global var
    var = 1
def func(x):
    global var
    var += 1
    redisgears.executeCommand('set',x['key'], var)
def OnTime():
    gearsCtx().foreach(func).collect().run()
gearsCtx().map(defineVar).collect().run()
redisgears.registerTimeEvent(2, OnTime)
    '''
    id = env.cmd('rg.pyexecute', script, 'UNBLOCKING')
    env.assertEqual(int(conn.get('x')), 1)
    env.assertEqual(int(conn.get('y')), 1)
    env.assertEqual(int(conn.get('z')), 1)
    time.sleep(3)
    env.assertTrue(int(conn.get('x')) >= 2)
    env.assertTrue(int(conn.get('y')) >= 2)
    env.assertTrue(int(conn.get('z')) >= 2)
    env.cmd('rg.dropexecution', id)


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
        time.sleep(1)
        res2 = env.cmd('get', 'x')
        env.assertTrue(int(res2) >= int(res1) + 1)

    env.expect('del', 'timeEvent').equal(1)
    res = env.cmd('keys', '*')
    env.assertEqual(set(res), set(['x']))
    res1 = env.cmd('get', 'x')
    time.sleep(1)
    res2 = env.cmd('get', 'x')
    env.assertTrue(int(res2) >= int(res1))
