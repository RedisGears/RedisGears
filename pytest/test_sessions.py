from RLTest import Env, Defaults
from common import getConnectionByEnv, verifyRegistrationIntegrity
from common import TimeLimit
import uuid
from includes import *
from common import gearsTest

@gearsTest()
def testSimpleSessionUpgrade(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test').ok
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test', mode='sync')", 'ID', 'test').error().contains('Session test already exists')
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test', mode='sync')", 'ID', 'test', 'UPGRADE').ok
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal(['1'])

@gearsTest()
def testSessionUpgradeOnNoneExistingSessios(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'UPGRADE').ok
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])

@gearsTest()
def testSessionAllCharsCombination(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'Test_1-1').ok
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])

@gearsTest()
def testSessionReplaceWith(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test').ok
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test', mode='sync')", 'ID', 'test1', 'REPLACE_WITH', 'test').ok
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal(['1'])

@gearsTest(skipOnCluster=True)
def testMissingSessionID(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID').error().contains('ID is missing')

@gearsTest(skipOnCluster=True)
def testReplaceWithSessionThatDoesNotExists(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'REPLACE_WITH', 'foo').error().contains('Can not replace with foo, session does not exists')

@gearsTest(skipOnCluster=True)
def testReplaceWithMissing(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'REPLACE_WITH').error().contains('REPLACE_WITH is missing')

@gearsTest(skipOnCluster=True)
def testDescriptionMissing(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'DESCRIPTION').error().contains('DESCRIPTION is missing')

@gearsTest(skipOnCluster=True)
def testUnknownArgument(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'UNKNOWN').error().contains('Unknown arguments were given')
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'UNKNOWN', 'doo').error().contains('Unknown arguments were given')

@gearsTest(skipOnCluster=True)
def testBadSessionId(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test@a').error().contains('must be compose of')

@gearsTest(skipOnCluster=True)
def testBadReplaceWith(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test').ok()
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test1')", 'ID', 'test1').ok()
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'REPLACE_WITH', 'test1').error().contains('Can not replace an existing session test with test1')

@gearsTest()
def testRequirementsWithZeroRequirementsAreSimplyIgnored(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'REQUIREMENTS').ok()
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])

@gearsTest()
def testBasicPyDumpSessions(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'DESCRIPTION', 'desc').ok()
    verifyRegistrationIntegrity(env)
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = c.execute_command('RG.PYDUMPSESSIONS')
        env.assertEqual(res[0][0:-1], ['ID', 'test', 'sessionDescription', 'desc', 'refCount', 1, 'linked', 'primary', 'dead', 'false', 'requirementInstallationNeeded', 0, 'requirements', [], 'registrations'])
        res = c.execute_command('RG.PYDUMPSESSIONS VERBOSE')
        if env.shardsCount == 1:
            env.assertEqual(res[0][-1][0][1], '0000000000000000000000000000000000000000-1')

@gearsTest(envArgs={'moduleArgs': 'CreateVenv 1'})
def testBasicPyDumpSessionsWithRequirements(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test')", 'ID', 'test', 'DESCRIPTION', 'desc', 'REQUIREMENTS', 'redis').ok()
    verifyRegistrationIntegrity(env)
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = c.execute_command('RG.PYDUMPSESSIONS')
        env.assertEqual(res[0][0:-1], ['ID', 'test', 'sessionDescription', 'desc', 'refCount', 1, 'linked', 'primary', 'dead', 'false', 'requirementInstallationNeeded', 1, 'requirements', ['redis'], 'registrations'])
        res = c.execute_command('RG.PYDUMPSESSIONS VERBOSE')
        env.assertEqual(res[0][13][0][3], 'redis')

@gearsTest()
def testBasicPyDumpSessionsList(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test1')", 'ID', 'test1', 'DESCRIPTION', 'desc').ok()
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='test2')", 'ID', 'test2', 'DESCRIPTION', 'desc').ok()
    verifyRegistrationIntegrity(env)
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = c.execute_command('RG.PYDUMPSESSIONS', 'SESSIONS', 'test1', 'test2')
        env.assertEqual(res[0][1], 'test1')
        env.assertEqual(res[1][1], 'test2')
        res = c.execute_command('RG.PYDUMPSESSIONS', 'SESSIONS', 'test1', 'test3')
        env.assertEqual(res[0][1], 'test1')
        env.assertEqual(res[1], 'Session test3 does not exists')

@gearsTest(skipOnCluster=True)
def testBasicPyDumpSessionsUnknowArg(env):
    env.expect('RG.PYDUMPSESSIONS', 'foo').error().contains('Unknown option')

@gearsTest(skipCleanups=True)
def testBasicPyDumpSessionsTS(env):
    script = '''
import asyncio
async def test(r):
    await asyncio.sleep(20)
    return 'OK'

GB('ShardsIDReader').map(test).run()
GB().register()
    '''
    env.expect('RG.PYEXECUTE', script, 'ID', 'test1', 'DESCRIPTION', 'desc', 'UNBLOCKING')
    env.expect('RG.PYEXECUTE', script, 'ID', 'test1', 'DESCRIPTION', 'desc', 'UNBLOCKING', 'UPGRADE')
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        with TimeLimit(2, env, 'Failed waiting for session to become TS'):
            while True:
                res = c.execute_command('RG.PYDUMPSESSIONS', 'DEAD')
                if len(res) == 1:
                    break
                time.sleep(0.1)

@gearsTest()
def testRegisterInsideRegister(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').foreach(lambda x: GB('CommandReader').map(lambda x: '+OK').register(trigger=x[1], mode='sync')).map(lambda x: '+OK').register(trigger='test', mode='sync')", 'ID', 'test', 'DESCRIPTION', 'desc').ok()
    verifyRegistrationIntegrity(env)
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = c.execute_command('RG.PYDUMPSESSIONS')
        env.assertEqual(len(res[0][-1]), 1)
    c = env.getConnection(env.shardsCount)
    res = c.execute_command('RG.TRIGGER', 'test', 'test1')
    env.assertEqual(res, ['OK'])
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        with TimeLimit(2, env, 'Failed waiting for registration to arrive shard %d' % i):
            while True:
                res = c.execute_command('RG.PYDUMPSESSIONS')
                if len(res[0][-1]) == 2:
                    break
                time.sleep(0.1)
    env.expect('RG.TRIGGER', 'test1').equal(['OK'])

@gearsTest()
def testRegistrationFailureRoleback(env):
    script = '''
GB('CommandReader').count().register(trigger='test1')
GB('CommandReader').count().register(trigger='test2')
failure()
    '''
    env.expect('RG.PYEXECUTE', script, 'ID', 'test', 'DESCRIPTION', 'desc').error().contains('is not defined')
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = c.execute_command('RG.PYDUMPSESSIONS', 'SESSIONS')
        env.assertEqual(len(res), 0)

@gearsTest()
def testRegistrationFailureRolebackToOriginalRegistrations(env):
    script1 = '''
GB('CommandReader').count().map(lambda x: 'test1 reports %s shards' % str(x)).register(trigger='test1')
GB('CommandReader').count().map(lambda x: 'test2 reports %s shards' % str(x)).register(trigger='test2')
    '''
    script2 = '''
GB('CommandReader').count().map(lambda x: 'new test1 reports %s shards' % str(x)).register(trigger='test1')
GB('CommandReader').count().map(lambda x: 'new test2 reports %s shards' % str(x)).register(trigger='test2')
failure()
    '''
    env.expect('RG.PYEXECUTE', script1, 'ID', 'test', 'DESCRIPTION', 'desc').ok()
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test1').equal(['test1 reports %s shards' % str(env.shardsCount)])
    env.expect('RG.TRIGGER', 'test2').equal(['test2 reports %s shards' % str(env.shardsCount)])
    env.expect('RG.PYEXECUTE', script2, 'ID', 'test', 'DESCRIPTION', 'desc', 'UPGRADE').error().contains('is not define')
    verifyRegistrationIntegrity(env)
    env.expect('RG.TRIGGER', 'test1').equal(['test1 reports %s shards' % str(env.shardsCount)])
    env.expect('RG.TRIGGER', 'test2').equal(['test2 reports %s shards' % str(env.shardsCount)])

@gearsTest(skipOnCluster=True)
def testCommandReaderRegisterSameTrigger(env):
    script = '''
GB('CommandReader').count().map(lambda x: 'test1 reports %s shards' % str(x)).register(trigger='test')
GB('CommandReader').count().map(lambda x: 'test2 reports %s shards' % str(x)).register(trigger='test')
    '''
    env.expect('RG.PYEXECUTE', script, 'ID', 'test', 'DESCRIPTION', 'desc').error().contains('trigger already registered in this session')

@gearsTest(skipOnCluster=True)
def testMod4054(env):
    script = """
dm_g1 = GearsBuilder()
dm_g1.register('prefix*', eventTypes=['set', 'change'], readValue=False, mode='sync')

m1_g2 = GearsBuilder('StreamReader')
m1_g2.register(prefix='foo:*', mode='async_local', trimStream=False) 

m2_g2 = GearsBuilder('StreamReader')
m2_g2.register(prefix='foo:*', mode='async_local', trimStream=False) 

m3_g2 = GearsBuilder('StreamReader')
m3_g2.register(prefix='foo:*', mode='async_local', trimStream=False) 

GB('CommandReader').register(trigger='bar')

GearsBuilder().register("key", eventTypes=['expired'], noScan=True, readValue=False, mode='sync')

GB('CommandReader').register(trigger='foo1')
GB('CommandReader').register(trigger='foo1')
    """
    env.expect('RG.PYEXECUTE', script, 'ID', 'test', 'DESCRIPTION', 'desc').error().contains('trigger already registered')

@gearsTest(skipOnCluster=True)
def testCommandReaderOverrideCommandWithNoneSyncRegistration(env):
    script = '''
GB('CommandReader').count().map(lambda x: 'test1 reports %s shards' % str(x)).register(hook='set')
GB('CommandReader').count().map(lambda x: 'test2 reports %s shards' % str(x)).register(hook='set')
    '''
    env.expect('RG.PYEXECUTE', script, 'ID', 'test', 'DESCRIPTION', 'desc').error().contains('Can not override a none sync registration which already created on this session')

@gearsTest(skipCleanups=True, skipCallback=lambda: Defaults.num_shards != 2)
def testSessionUpgradeFailureOnShard(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')", 'ID', 'test', 'DESCRIPTION', 'desc').ok()
    # restart shard 2
    conn1 = env.getConnection(shardId=1)
    conn2 = env.getConnection(shardId=2)
    conn2.execute_command('config', 'set', 'save', '')
    env.envRunner.shards[1].stopEnv()
    env.envRunner.shards[1].startEnv()
    env.envRunner.waitCluster()
    # put a session only on the second shard
    conn2.execute_command('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')", 'ID', 'test1', 'DESCRIPTION', 'desc')
    getConnectionByEnv(env) # will make sure to re-enable the cluster
    res = env.cmd('RG.PYEXECUTE', "GB('CommandReader').register(trigger='test')", 'ID', 'test', 'DESCRIPTION', 'desc', 'UPGRADE')
    env.assertContains('trigger already registered', str(res[0]))

@gearsTest(skipCallback=lambda: Defaults.num_shards != 2)
def testSessionUpgradeSuccessedOnMissingRegistration(env):
    env.expect('RG.PYEXECUTE', 'GB().register()', 'ID', 'test', 'DESCRIPTION', 'desc').ok()
    # restart shard 2
    conn1 = env.getConnection(shardId=1)
    conn2 = env.getConnection(shardId=2)
    conn2.execute_command('config', 'set', 'save', '')
    env.envRunner.shards[1].stopEnv()
    env.envRunner.shards[1].startEnv()
    env.envRunner.waitCluster()
    getConnectionByEnv(env) # will make sure to re-enable the cluster
    env.expect('RG.PYEXECUTE', 'GB().register()', 'ID', 'test', 'DESCRIPTION', 'desc1', 'UPGRADE').ok()
