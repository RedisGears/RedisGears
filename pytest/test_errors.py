from RLTest import Env


def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn


def testInvalidSyntax(env):
    env.expect('rg.pyexecute', '1defs + GearsBuilder().notexists()').error().contains("invalid syntax")


def testScriptError(env):
    env.expect('rg.pyexecute', 'GearsBuilder().notexists()').error().equal("GearsBuilder instance has no attribute 'notexists'")


def testForEachError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().foreach(lambda x: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testGroupByError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().groupby(lambda x: "str", lambda a, x, k: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testBatchGroupByError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().batchgroupby(lambda x: "str", lambda x, k: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testExtractorError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().groupby(lambda x: notexists(x), lambda a, x, k: 1).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testAccumulateError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().accumulate(lambda a, x: notexists(a, x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testMapError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().map(lambda x: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testFlatMapError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().flatmap(lambda x: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testFilterError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().filter(lambda x: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testRepartitionError(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '1')
    res = env.cmd('rg.pyexecute', 'GearsBuilder().repartition(lambda x: notexists(x)).repartition(lambda x: notexists(x)).collect().run()')
    env.assertContains("global name 'notexists' is not defined", res[1][0])


def testMapWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().map(1, 2).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().map(1).run()').error().contains('argument must be a function')


def testFilterWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().filter(1, 2).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().filter(1).run()').error().contains('argument must be a function')


def testGroupByWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().groupby(1, 2, 3).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().groupby(1, 2).run()').error().contains('argument must be a function')


def testBatchGroupByWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().batchgroupby(1, 2, 3).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().batchgroupby(1, 2).run()').error().contains('argument must be a function')


def testCollectWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().collect(1, 2, 3).run()').error().contains('wrong number of args')


def testForEachWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().foreach(1, 2).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().foreach(1).run()').error().contains('argument must be a function')


def testRepartitionWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().repartition(1, 2).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().repartition(1).run()').error().contains('argument must be a function')


def testLimitWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().limit().run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().limit(1, 2, 3).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().limit("awdwada").run()').error().contains('argument must be a number')
    env.expect('rg.pyexecute', 'GB().limit(1, "kakaka").run()').error().contains('argument must be a number')


def testAccumulateWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().accumulate(1, 2).run()').error().contains('wrong number of args')
    env.expect('rg.pyexecute', 'GB().accumulate(1).run()').error().contains('argument must be a function')


def testAvgWrongArgs(env):
    env.expect('rg.pyexecute', 'GB().avg(1).run()').error().contains('argument must be a function')


def testBuilderCreationWithUnexistingReader(env):
    env.expect('rg.pyexecute', 'GB("unexists").accumulate(lambda a, x: 1 + (a if a else 0)).run()').error().contains('reader are not exists')


def testPyReaderWithWrongArgument(env):
    env.expect('rg.pyexecute', 'GB("PythonReader").run("*")').error().contains('pyreader argument must be a functio')
    env.expect('rg.pyexecute', 'GB("PythonReader").run()').error().contains('pyreader argument must be a functio')
    env.expect('rg.pyexecute', 'GB("PythonReader", "*").run()').error().contains('pyreader argument must be a functio')
    env.expect('rg.pyexecute', 'GB("PythonReader", PythonReaderCallback).run("*")').error().contains('pyreader argument must be a functio')
