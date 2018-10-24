from RLTest import Env
import yaml


class testBasic:
    def __init__(self):
        self.env = Env()
        for i in range(100):
            self.env.cmd('set', str(i), str(i))

    def testBasicQuery(self):
        res = self.env.cmd('execute', 'starCtx("*").returnResults(lambda x:str(x))')
        res = [yaml.load(r) for r in res]
        for i in range(100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)

    def testBasicFilterQuery(self):
        res = self.env.cmd('execute', 'starCtx("*").filter(lambda x: x["value"] >= 50).returnResults(lambda x:str(x))')
        res = [yaml.load(r) for r in res]
        for i in range(50, 100):
            self.env.assertContains({'value': str(i), 'key': str(i)}, res)

    def testBasicMapQuery(self):
        res = self.env.cmd('execute', 'starCtx("*").map(lambda x: x["value"]).returnResults(lambda x:str(x))')
        res = [yaml.load(r) for r in res]
        self.env.assertEqual(set(res), set([i for i in range(100)]))

    def testBasicGroupByQuery(self):
        res = self.env.cmd('execute', 'starCtx("*").'
                                      'map(lambda x: {"key":x["key"], "value": 0 if int(x["value"]) < 50 else 100}).'
                                      'groupby(lambda x: str(x["value"]), lambda key, vals: len(vals)).'
                                      'returnResults(lambda x:str(x))')
        self.env.assertContains("{'value': 50, 'key': '100'}", res)
        self.env.assertContains("{'value': 50, 'key': '0'}", res)
