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
