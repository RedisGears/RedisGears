from RLTest import Env
from common import getConnectionByEnv
from common import TimeLimit
import uuid
from includes import *
from common import gearsTest

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesInstall(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                            "map(lambda x: str(__import__('redis')))."
                            "collect().distinct().run()", 'REQUIREMENTS', 'redis==3')
    env.assertEqual(len(res[0]), env.shardsCount)
    env.assertEqual(len(res[1]), 0)
    env.assertContains("<module 'redis'", res[0][0])

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesInstallWithVersionGreater(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                            "map(lambda x: str(__import__('redis')))."
                            "collect().distinct().run()", 'REQUIREMENTS', 'redis>=2,<=3')
    env.assertEqual(len(res[0]), env.shardsCount)
    env.assertEqual(len(res[1]), 0)
    env.assertContains("<module 'redis'", res[0][0])

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesInstallWithVersionEqual(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                            "map(lambda x: str(__import__('redis')))."
                            "collect().distinct().run()", 'REQUIREMENTS', 'redis==3')
    env.assertEqual(len(res[0]), env.shardsCount)
    env.assertEqual(len(res[1]), 0)
    env.assertContains("<module 'redis'", res[0][0])

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesInstallFailure(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('ShardsIDReader')."
                               "map(lambda x: __import__('redisgraph'))."
                               "collect().distinct().run()", 'REQUIREMENTS', str(uuid.uuid4())).error().contains('satisfy requirements')

@gearsTest(skipOnCluster=True, envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesWithRegister(env):
    env.expect('RG.PYEXECUTE', "GB()."
                               "map(lambda x: __import__('redis'))."
                               "collect().distinct().register()", 'REQUIREMENTS', 'redis==3').ok()

    for _ in env.reloading_iterator():
        res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader')."
                                      "map(lambda x: str(__import__('redis')))."
                                      "collect().distinct().run()")
        env.assertEqual(len(res[0]), env.shardsCount)
        env.assertEqual(len(res[1]), 0)
        env.assertContains("<module 'redis'", res[0][0])

@gearsTest(decodeResponses=False, envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesBasicExportImport(env):
    conn = getConnectionByEnv(env)

    #disable rdb save
    res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').foreach(lambda x: execute('config', 'set', 'save', '')).run()")
    
    env.expect('RG.PYEXECUTE', "GB().register()", 'REQUIREMENTS', 'redis').equal(b'OK')
    md, data = env.cmd('RG.PYEXPORTREQ', 'redis')
    env.assertEqual(md[5], b'yes')
    env.assertEqual(md[7], b'yes')
    env.stop()
    env.start()
    conn = getConnectionByEnv(env)
    env.expect('RG.PYDUMPREQS').equal([])
    env.expect('RG.PYIMPORTREQ', *data).equal(b'OK')
    res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').flatmap(lambda x: execute('RG.PYDUMPREQS')).run()")
    env.assertEqual(len(err), 0)
    env.assertEqual(len(res), env.shardsCount)
    for r in res:
        env.assertContains("'IsDownloaded', 'yes', 'IsInstalled', 'yes'", r.decode('utf8'))

@gearsTest(envArgs={'useSlaves':True, 'env':'oss', 'moduleArgs':'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesReplicatedToSlave(env):
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication

    env.expect('RG.PYEXECUTE', "GB().register()", 'REQUIREMENTS', 'redis').ok()

    slaveConn = env.getSlaveConnection()
    try:
        with TimeLimit(60):
            res = []
            while len(res) < 1:
                res = slaveConn.execute_command('RG.PYDUMPREQS')
            env.assertEqual(len(res), 1)
            env.assertEqual(res[0][5], 'yes')
            env.assertEqual(res[0][7], 'yes')
    except Exception:
        env.assertTrue(False, message='Failed waiting for requirement to reach slave')

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1', 'freshEnv': True}, executionMaxIdleTime=60000)
def testDependenciesSavedToRDB(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB().register()", 'REQUIREMENTS', 'redis').ok()
    for _ in env.reloading_iterator():
        res, err = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').flatmap(lambda x: execute('RG.PYDUMPREQS')).run()")
        env.assertEqual(len(err), 0)
        env.assertEqual(len(res), env.shardsCount)
        for r in res:
            env.assertContains("'IsDownloaded', 'yes', 'IsInstalled', 'yes'", r)

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1', 'useAof':True}, executionMaxIdleTime=60000)
def testAof(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB().register()", 'REQUIREMENTS', 'redis').ok()

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

@gearsTest(skipCleanups=True, decodeResponses=False, envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesImportSerializationError(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB().register()", 'REQUIREMENTS', 'rejson', 'redis==3').equal(b'OK')
    md, data = env.cmd('RG.PYEXPORTREQ', 'rejson')
    for i in range(len(data) - 1):
        env.expect('RG.PYIMPORTREQ', data[:i]).error()

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesForceUpgrade(env):
    getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "import redis;GB('CommandReader').map(lambda x: id(redis)).register(trigger='test')", 'ID', 'test', 'REQUIREMENTS', 'redis==3').equal('OK')
    res1 = env.cmd('RG.TRIGGER', "test")
    res1.sort()

    paths1 = []
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        reqs = c.execute_command('rg.pydumpreqs')
        env.assertEqual(len(reqs), 1)
        paths1.append(reqs[0][13])
    
    env.expect('RG.PYEXECUTE', "import redis;GB('CommandReader').map(lambda x: id(redis)).register(trigger='test')", 'ID', 'test', 'UPGRADE', 'REQUIREMENTS', 'redis==3').equal('OK')
    res2 = env.cmd('RG.TRIGGER', "test")
    res2.sort()
    env.assertEqual(res1, res2)
    
    paths2 = []
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        reqs = c.execute_command('rg.pydumpreqs')
        env.assertEqual(len(reqs), 1)
        paths2.append(reqs[0][13])

    paths1.sort()
    paths2.sort()
    env.assertEqual(paths1, paths2)

    env.expect('RG.PYEXECUTE', "import redis;GB('CommandReader').map(lambda x: id(redis)).register(trigger='test')", 'ID', 'test', 'UPGRADE', 'FORCE_REINSTALL_REQUIREMENTS', 'REQUIREMENTS', 'redis==3').equal('OK')
    res3 = env.cmd('RG.TRIGGER', "test")
    res3.sort()
    env.assertNotEqual(res3, res2)

    paths3 = []
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        reqs = c.execute_command('rg.pydumpreqs')
        env.assertEqual(len(reqs), 1)
        paths3.append(reqs[0][13])

    paths3.sort()
    env.assertNotEqual(paths3, paths2)

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'}, executionMaxIdleTime=60000)
def testDependenciesForceUpgradeFailure(env):
    getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "import redis;GB('CommandReader').map(lambda x: id(redis)).register(trigger='test')", 'ID', 'test', 'REQUIREMENTS', 'redis==3').equal('OK')
    paths1 = []
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        reqs = c.execute_command('rg.pydumpreqs')
        env.assertEqual(len(reqs), 1)
        paths1.append(reqs[0][13])
    env.expect('RG.PYEXECUTE', "import redis1;GB('CommandReader').map(lambda x: id(redis)).register(trigger='test')", 'ID', 'test', 'UPGRADE', 'FORCE_REINSTALL_REQUIREMENTS', 'REQUIREMENTS', 'redis==3').error()
    paths2 = []
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        reqs = c.execute_command('rg.pydumpreqs')
        env.assertEqual(len(reqs), 1)
        paths2.append(reqs[0][13])
    paths1.sort()
    paths2.sort()
    env.assertEqual(paths1, paths2)
