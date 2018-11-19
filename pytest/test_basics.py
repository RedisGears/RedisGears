from RLTest import Env
import yaml
import time


def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn


class testBasic:
    def __init__(self):
        self.env = Env()
        self.env.broadcast('rs.refreshcluster')
        conn = getConnectionByEnv(self.env)
        for i in range(100):
            conn.execute_command('set', str(i), str(i))

    def testBasicQuery(self):
        id = self.env.cmd('rs.pyexecute', "starCtx('test1').map(lambda x:str(x)).collect().run()")
        res = self.env.cmd('RS.getresultsblocking', id)
        res = [yaml.load(r) for r in res]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rs.dropexecution', id)

    def testBasicFilterQuery(self):
        id = self.env.cmd('rs.pyexecute', 'starCtx("test2").filter(lambda x: int(x["value"]) >= 50).map(lambda x:str(x)).collect().run()')
        res = self.env.cmd('rs.getresultsblocking', id)
        res = [yaml.load(r) for r in res]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)
        self.env.cmd('rs.dropexecution', id)

    def testBasicMapQuery(self):
        id = self.env.cmd('rs.pyexecute', 'starCtx("test3").map(lambda x: x["value"]).map(lambda x:str(x)).collect().run()')
        res = self.env.cmd('rs.getresultsblocking', id)
        res = [yaml.load(r) for r in res]
        self.env.assertEqual(set(res), set([i for i in range(100)]))
        self.env.cmd('rs.dropexecution', id)

    def testBasicGroupByQuery(self):
        id = self.env.cmd('rs.pyexecute', 'starCtx("test4").'
                                          'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                          'groupby(lambda x: str(x["value"]), lambda key, vals: len(vals)).'
                                          'map(lambda x:str(x)).collect().run()')
        res = self.env.cmd('rs.getresultsblocking', id)
        self.env.assertContains("{'value': 50, 'key': '100'}", res)
        self.env.assertContains("{'value': 50, 'key': '0'}", res)
        self.env.cmd('rs.dropexecution', id)


def testFlatMap(env):
    env.broadcast('rs.refreshcluster')
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rs.pyexecute', "starCtx('test')."
                                 "flatMap(lambda x: x['value'])."
                                 "collect().run()")
    res = env.cmd('rs.getresultsblocking', id)
    env.assertEqual(set(res), set(['1', '2', '3']))
    env.cmd('rs.dropexecution', id)


def testLimit(env):
    env.broadcast('rs.refreshcluster')
    conn = getConnectionByEnv(env)
    conn.execute_command('lpush', 'l', '1', '2', '3')
    id = env.cmd('rs.pyexecute', "starCtx('test')."
                                 "flatMap(lambda x: x['value'])."
                                 "limit(1).collect().run()")
    res = env.cmd('rs.getresultsblocking', id)
    env.assertEqual(len(res), 1)
    env.cmd('rs.dropexecution', id)


def testRepartitionAndWriteOption(env):
    env.broadcast('rs.refreshcluster')
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    id = env.cmd('rs.pyexecute', "starCtx('test')."
                                 "repartition(lambda x: x['value'])."
                                 "write(lambda x: redistar.saveKey(x['value'], x['key']))."
                                 "map(lambda x : str(x)).collect().run()")
    res = env.cmd('rs.getresultsblocking', id)
    env.assertContains("'value': '1'", str(res))
    env.assertContains("'key': 'x'", str(res))
    env.assertContains("'value': '2'", str(res))
    env.assertContains("'key': 'y'", str(res))
    env.assertContains("'value': '3'", str(res))
    env.assertContains("'key': 'z'", str(res))
    env.assertEqual(conn.execute_command('get', '1'), 'x')
    env.assertEqual(conn.execute_command('get', '2'), 'y')
    env.assertEqual(conn.execute_command('get', '3'), 'z')
    env.cmd('rs.dropexecution', id)


def testBasicStream(env):
    env.broadcast('rs.refreshcluster')
    conn = getConnectionByEnv(env)
    env.expect('rs.pyexecute', "starStreamingCtx('test')."
                               "repartition(lambda x: x['value'])."
                               "write(lambda x: redistar.saveKey(x['value'], x['key']))."
                               "register()").ok()
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    res = []
    while len(res) != 3:
        res = env.cmd('rs.dumpexecutions')
    for e in res:
        env.broadcast('rs.getresultsblocking', e[3])
        env.cmd('rs.dropexecution', e[3])
    env.assertEqual(conn.get('x'), '1')
    env.assertEqual(conn.get('y'), '2')
    env.assertEqual(conn.get('z'), '3')
    env.assertEqual(conn.get('1'), 'x')
    env.assertEqual(conn.get('2'), 'y')
    env.assertEqual(conn.get('3'), 'z')
