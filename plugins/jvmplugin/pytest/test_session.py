from common import jvmTestDecorator, verifyRegistrationIntegrity, putKeys, BASE_JAR_FILE, toDictionary
import os

@jvmTestDecorator()
def testSimpleSessionUpgrade(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(results, 'OK')

    env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])

    with open(BASE_JAR_FILE, 'rb') as f:
        data = f.read()
        res = env.expect('RG.JEXECUTE', 'gears_tests.testSimpleSessionUpgrade', data).error().contains('already exists')
        env.expect('RG.JEXECUTE', 'gears_tests.testSimpleSessionUpgrade', 'UPGRADE', data).ok()
        verifyRegistrationIntegrity(env)
        env.expect('RG.TRIGGER', 'test').equal([str(env.shardsCount)])

@jvmTestDecorator()
def testSessionUpgradeWithVersionAndDescription(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(results, 'OK')

    env.expect('RG.TRIGGER', 'test').equal(["OK"])

    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = toDictionary(c.execute_command('RG.JDUMPSESSIONS')[0])
        env.assertEqual(res['mainClass'], 'gears_tests.testSessionUpgradeWithVersionAndDescription')
        env.assertEqual(res['version'], 1)
        env.assertEqual(res['description'], 'foo')

    with open(BASE_JAR_FILE, 'rb') as f:
        data = f.read()
        res = env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithVersionAndDescription', data).error().contains('already exists')
        res = env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithVersionAndDescription', 'UPGRADE', data).error().contains('Session with higher (or equal) version already exists')
        env.expect('RG.CONFIGSET', 'REQUESTED_VERSION', '2').equal(['OK - value was saved in extra config dictionary'])
        env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithVersionAndDescription', 'UPGRADE', data).ok()
        env.expect('RG.CONFIGSET', 'REQUESTED_VERSION', '1').equal(['OK - value was saved in extra config dictionary'])
        env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithVersionAndDescription', 'UPGRADE', data).error().contains('Session with higher (or equal) version already exists')
        env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithVersionAndDescription', 'UPGRADE', 'FORCE', data).ok()

@jvmTestDecorator()
def testSessionUpgradeWithUpgradeData(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(results, 'OK')

    env.expect('RG.TRIGGER', 'get_upgrade_data').equal(["no upgrade data"])

    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = toDictionary(c.execute_command('RG.JDUMPSESSIONS')[0])
        env.assertEqual(res['version'], 1)
        env.assertEqual(res['upgradeData'], None)

    with open(BASE_JAR_FILE, 'rb') as f:
        data = f.read()
        env.expect('RG.CONFIGSET', 'REQUESTED_VERSION', '2').equal(['OK - value was saved in extra config dictionary'])
        env.expect('RG.CONFIGSET', 'UPGRADE_EXCEPTION', 'enabled').equal(['OK - value was saved in extra config dictionary'])
        env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithUpgradeData', 'UPGRADE', data).error().contains('Upgrade Exception')
        env.expect('RG.CONFIGSET', 'UPGRADE_EXCEPTION', 'disabled').equal(['OK - value was saved in extra config dictionary'])
        res = env.expect('RG.JEXECUTE', 'gears_tests.testSessionUpgradeWithUpgradeData', 'UPGRADE', data).ok()
        verifyRegistrationIntegrity(env)

        for i in range(1, env.shardsCount + 1, 1):
            c = env.getConnection(i)
            res = c.execute_command('RG.TRIGGER', 'get_upgrade_data')
            env.assertEqual(res, ['some upgrade data'])
            res = toDictionary(c.execute_command('RG.JDUMPSESSIONS')[0])
            env.assertEqual(res['version'], 2)
            env.assertEqual(res['upgradeData'], 'some upgrade data')

@jvmTestDecorator()
def testJDumpSessions(env, results, errs, **kargs):
    env.assertEqual(len(errs), 0)
    env.assertEqual(results, 'OK')

    env.expect('RG.JDUMPSESSIONS', 'BAD_ARGUMENT').error().contains('Unknown option BAD_ARGUMENT')
    env.expect('RG.JDUMPSESSIONS', 'BAD_ARGUMENT', 'SESSIONS', 'gears_tests.testJDumpSessions').error().contains('Unknown option BAD_ARGUMENT')
    env.expect('RG.JDUMPSESSIONS', 'SESSIONS', 'no_exists').equal(['Session no_exists does not exists'])

    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = toDictionary(c.execute_command('RG.JDUMPSESSIONS')[0])
        env.assertEqual(res['mainClass'], 'gears_tests.testJDumpSessions')
        env.assertEqual(len(res['registrations']), 1)
        res = toDictionary(c.execute_command('RG.JDUMPSESSIONS', 'VERBOSE', 'SESSIONS', 'gears_tests.testJDumpSessions')[0])
        env.assertEqual(res['mainClass'], 'gears_tests.testJDumpSessions')
        env.assertEqual(len(res['registrations']), 1)

    with open(BASE_JAR_FILE, 'rb') as f:
        data = f.read()
        res = env.expect('RG.JEXECUTE', 'gears_tests.testJDumpSessions', 'UPGRADE', data).ok()
        for i in range(1, env.shardsCount + 1, 1):
            c = env.getConnection(i)
            res = toDictionary(c.execute_command('RG.JDUMPSESSIONS', 'DEAD')[0])
            env.assertEqual(res['mainClass'], 'gears_tests.testJDumpSessions')
            env.assertEqual(len(res['registrations']), 0)
