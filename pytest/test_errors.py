from RLTest import Env


def testInvalidSyntax(env):
    env.skipOnCluster()
    env.expect('rg.pyexecute', '1defs + gearsCtx().notexists()').error().contains("invalid syntax")


def testScriptError(env):
    env.skipOnCluster()
    env.expect('rg.pyexecute', 'gearsCtx().notexists()').error().equal("'redisgears.PyFlatExecution' object has no attribute 'notexists'")


def testForEachError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().foreach(lambda x: notexists(x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testGroupByError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().groupby(lambda x: "str", lambda a, x, k: notexists(x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testBatchGroupByError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().batchgroupby(lambda x: "str", lambda x, k: notexists(x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testExtractorError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().groupby(lambda x: notexists(x), lambda a, x, k: 1).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testAccumulateError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().accumulate(lambda a, x: notexists(a, x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testMapError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().map(lambda x: notexists(x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testFlatMapError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().flatmap(lambda x: notexists(x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])


def testFilterError(env):
    env.skipOnCluster()
    env.cmd('set', 'x', '1')
    res = env.cmd('rg.pyexecute', 'gearsCtx().filter(lambda x: notexists(x)).run()')
    env.assertContains("global name 'notexists' is not defined", res[1])
