from RLTest import Env
from common import getConnectionByEnv
from common import TimeLimit
import uuid

def testDependenciesInstall():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                            "map(lambda x: str(__import__('redisgraph')))."
                            "collect().distinct().run()", 'REQUIREMENTS', 'redisgraph')
    env.assertEqual(len(res[0]), env.shardsCount)
    env.assertEqual(len(res[1]), 0)
    env.assertContains("<module 'redisgraph'", res[0][0])

def testDependenciesInstallWithVersionGreater():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                            "map(lambda x: str(__import__('redis')))."
                            "collect().distinct().run()", 'REQUIREMENTS', 'redis>=3')
    env.assertEqual(len(res[0]), env.shardsCount)
    env.assertEqual(len(res[1]), 0)
    env.assertContains("<module 'redis'", res[0][0])

def testDependenciesInstallWithVersionEqual():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                            "map(lambda x: str(__import__('redis')))."
                            "collect().distinct().run()", 'REQUIREMENTS', 'redis==3')
    env.assertEqual(len(res[0]), env.shardsCount)
    env.assertEqual(len(res[1]), 0)
    env.assertContains("<module 'redis'", res[0][0])

def testDependenciesInstallFailure():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('ShardsIDReader')."
                               "map(lambda x: __import__('redisgraph'))."
                               "collect().distinct().run()", 'REQUIREMENTS', str(uuid.uuid4())).error().contains('satisfy requirments')

def testDependenciesWithRegister():
    env = Env(moduleArgs='CreateVenv 1')
    env.skipOnCluster()
    env.expect('RG.PYEXECUTE', "GB()."
                               "map(lambda x: __import__('redisgraph'))."
                               "collect().distinct().register()", 'REQUIREMENTS', 'redisgraph').ok()

    for _ in env.reloading_iterator():
        res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                                      "map(lambda x: str(__import__('redisgraph')))."
                                      "collect().distinct().run()")
        env.assertEqual(len(res[0]), env.shardsCount)
        env.assertEqual(len(res[1]), 0)
        env.assertContains("<module 'redisgraph'", res[0][0])

def testDependenciesBasicExportImport():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "import redisgraph", 'REQUIREMENTS', 'redisgraph').ok()
    md, data = env.cmd('RG.PYEXPORTREQ', 'redisgraph')
    env.assertEqual(md[5], 'yes')
    env.assertEqual(md[7], 'yes')
    env.stop()
    env.start()
    conn = getConnectionByEnv(env)
    env.expect('RG.PYDUMPREQS').equal([])
    env.expect('RG.PYIMPORTREQ', *data).equal('OK')
    res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').flatmap(lambda x: execute('RG.PYDUMPREQS')).run()")
    env.assertEqual(len(err), 0)
    env.assertEqual(len(res), env.shardsCount)
    for r in res:
        env.assertContains("'IsDownloaded', 'yes', 'IsInstalled', 'yes'", r)

def testDependenciesReplicatedToSlave():
    env = Env(useSlaves=True, env='oss', moduleArgs='CreateVenv 1')
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication

    env.expect('RG.PYEXECUTE', "import redisgraph", 'REQUIREMENTS', 'redisgraph').ok()

    slaveConn = env.getSlaveConnection()
    try:
        with TimeLimit(5):
            res = []
            while len(res) < 1:
                res = slaveConn.execute_command('RG.PYDUMPREQS')
            env.assertEqual(len(res), 1)
            env.assertEqual(res[0][5], 'yes')
            env.assertEqual(res[0][7], 'yes')
    except Exception:
        env.assertTrue(False, message='Failed waiting for requirement to reach slave')

def testDependenciesSavedToRDB():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "import redisgraph", 'REQUIREMENTS', 'redisgraph').ok()
    for _ in env.reloading_iterator():
        res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').flatmap(lambda x: execute('RG.PYDUMPREQS')).run()")
        env.assertEqual(len(err), 0)
        env.assertEqual(len(res), env.shardsCount)
        for r in res:
            env.assertContains("'IsDownloaded', 'yes', 'IsInstalled', 'yes'", r)

def testAof(env):
    env = Env(moduleArgs='CreateVenv 1', useAof=True)
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "import redisgraph", 'REQUIREMENTS', 'redisgraph').ok()

    res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').flatmap(lambda x: execute('RG.PYDUMPREQS')).run()")
    env.assertEqual(len(err), 0)
    env.assertEqual(len(res), env.shardsCount)
    for r in res:
        env.assertContains("'IsDownloaded', 'yes', 'IsInstalled', 'yes'", r)

    env.broadcast('debug', 'loadaof')

    res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').flatmap(lambda x: execute('RG.PYDUMPREQS')).run()")
    env.assertEqual(len(err), 0)
    env.assertEqual(len(res), env.shardsCount)
    for r in res:
        env.assertContains("'IsDownloaded', 'yes', 'IsInstalled', 'yes'", r)    

def testDependenciesImportSerializationError():
    env = Env(moduleArgs='CreateVenv 1')
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "import redisgraph", 'REQUIREMENTS', 'redisgraph').ok()
    md, data = env.cmd('RG.PYEXPORTREQ', 'redisgraph')
    data = b''.join(data)
    for i in range(len(data) - 1):
        env.expect('RG.PYIMPORTREQ', data[:i]).error()
