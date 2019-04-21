from RLTest import Env
import time

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

class testGenericErrors:
    def __init__(self):
        self.env = Env()


    def testInvalidSyntax(self):
        self.env.expect('rg.pyexecute', '1defs + GearsBuilder().notexists()').error().contains("invalid syntax")


    def testScriptError(self):
        self.env.expect('rg.pyexecute', 'GearsBuilder().notexists()').error().equal("GearsBuilder instance has no attribute 'notexists'")


    def testBuilderCreationWithUnexistingReader(self):
        self.env.expect('rg.pyexecute', 'GB("unexists").accumulate(lambda a, x: 1 + (a if a else 0)).run()').error().contains('reader are not exists')


class testStepsErrors:
    def __init__(self):
        self.env = Env()
        conn = getConnectionByEnv(self.env)
        conn.execute_command('set', 'x', '1')
        conn.execute_command('set', 'y', '1')

    def testForEachError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().foreach(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testGroupByError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().groupby(lambda x: "str", lambda a, x, k: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testBatchGroupByError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().batchgroupby(lambda x: "str", lambda x, k: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testExtractorError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().groupby(lambda x: notexists(x), lambda a, x, k: 1).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testAccumulateError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().accumulate(lambda a, x: notexists(a, x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testMapError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().map(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testFlatMapError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().flatmap(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testFilterError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().filter(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


    def testRepartitionError(self):
        res = self.env.cmd('rg.pyexecute', 'GearsBuilder().repartition(lambda x: notexists(x)).repartition(lambda x: notexists(x)).collect().run()')
        self.env.assertLessEqual(1, res[1])


class testStepsWrongArgs:
    def __init__(self):
        self.env = Env()


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
        self.env.expect('rg.pyexecute', 'GB("PythonReader", ShardReaderCallback).run("*")').error().contains('pyreader argument must be a functio')

class testGetExecutionErrorReporting:
    def __init__(self):
        self.env = Env()
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
            self.env.assertContains("global name 'notexists' is not defined", error)
        self.env.cmd('RG.DROPEXECUTION', id)


    def testErrorShouldBeReportedWithTracebackNotAttempted(self):
        self.env.cmd('RG.CONFIGSET', 'PythonAttemptTraceback', 0)
        id = self.env.cmd('RG.PYEXECUTE', 'GearsBuilder().repartition(lambda x: notexists(x)).repartition(lambda x: notexists(x)).collect().run()', 'UNBLOCKING')
        time.sleep(1)
        res = self.env.cmd('RG.GETEXECUTION', id)
        errors = res[0][3][9]
        for error in errors:
            self.env.assertContains("global name 'notexists' is not defined", error)
        self.env.cmd('RG.DROPEXECUTION', id)
        self.env.cmd('RG.CONFIGSET', 'PythonAttemptTraceback', 1)
