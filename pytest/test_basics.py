from RLTest import Env
import yaml


class testBasic:
    def __init__(self):
        self.env = Env()
        self.env.broadcast('rs.refreshcluster')
        for i in range(100):
            conn = None
            if self.env.env == 'oss-cluster':
                conn = self.env.envRunner.getClusterConnection()
            else:
                conn = self.env.getConnection()
            conn.execute_command('set', str(i), str(i))

    def testBasicQuery(self):
        self.env.expect('rs.execute', "starCtx('test1', '*').map(lambda x:str(x)).collect().run()").ok()
        res = self.env.cmd('RS.getresultsblocking', 'test1')
        res = [yaml.load(r) for r in res]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)

    def testBasicFilterQuery(self):
        self.env.expect('rs.execute', 'starCtx("test2", "*").filter(lambda x: int(x["value"]) >= 50).map(lambda x:str(x)).collect().run()').ok()
        res = self.env.cmd('rs.getresultsblocking', 'test2')
        res = [yaml.load(r) for r in res]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)

    def testBasicMapQuery(self):
        self.env.expect('rs.execute', 'starCtx("test3", "*").map(lambda x: x["value"]).map(lambda x:str(x)).collect().run()').ok()
        res = self.env.cmd('rs.getresultsblocking', 'test3')
        res = [yaml.load(r) for r in res]
        self.env.assertEqual(set(res), set([i for i in range(100)]))

    def testBasicGroupByQuery(self):
        self.env.expect('rs.execute', 'starCtx("test4", "*").'
                                      'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                      'groupby(lambda x: str(x["value"]), lambda key, vals: len(vals)).'
                                      'map(lambda x:str(x)).collect().run()').ok()
        res = self.env.cmd('rs.getresultsblocking', 'test4')
        self.env.assertContains("{'value': 50, 'key': '100'}", res)
        self.env.assertContains("{'value': 50, 'key': '0'}", res)
