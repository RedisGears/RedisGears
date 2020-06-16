from common import jvmTestDecorator
from common import putKeys
from common import TimeLimit
import time

@jvmTestDecorator()
def testBasicRegistration(env, results, conn, **kargs):
    env.assertEqual(results, 'OK')
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    res = []
    while len(res) < 6:
        res = env.cmd('rg.dumpexecutions')
    for e in res:
        env.broadcast('rg.getresultsblocking', e[1])
        env.cmd('rg.dropexecution', e[1])
    env.assertEqual(set(conn.lrange('values', '0', '-1')), set(['1', '2', '3']))

@jvmTestDecorator()
def testBasicStreamRegistration(env, results, conn, **kargs):
    conn.execute_command('xadd', 'stream1', '*', 'name', 'test')
    res = []

    try:
        with TimeLimit(5):
            res = ''
            while res is None or "{'name': 'test'}" not in res:
                res = conn.get('new_key')
    except Exception as e:
        env.assertTrue(False, message='Failed get correct data from new_key')

    conn.execute_command('xadd', 'stream2', '*', 'name', 'test1')

    try:
        with TimeLimit(5):
            res = ''
            while res is None or "{'name': 'test1'}" not in res:
                res = conn.get('new_key')
    except Exception:
        env.assertTrue(False, message='Failed get correct data from new_key')

    conn.execute_command('xadd', 'rstream1', '*', 'name', 'test2')
    env.assertContains("{'name': 'test1'}", conn.get('new_key'))

@jvmTestDecorator()
def testRegistersOnPrefix(env, results, conn, **kargs):
    conn.set('pref1:x', '1')
    conn.set('pref1:y', '2')
    conn.set('pref1:z', '3')

    res = []
    while len(res) < 3:
        res = env.cmd('rg.dumpexecutions')
        res = [r for r in res if r[3] == 'done']

    env.assertEqual(conn.get('pref2:x'), '1')
    env.assertEqual(conn.get('pref2:y'), '2')
    env.assertEqual(conn.get('pref2:z'), '3')

@jvmTestDecorator()
def testRegistersSurviveRestart(env, results, conn, **kargs):
    for _ in env.reloading_iterator():
        for i in range(20):
            conn.set(str(i), str(i))

        res = 0
        while res < 40:
            res = int(env.cmd('rg.pyexecute', "GB('ShardsIDReader').map(lambda x: len([r for r in execute('rg.dumpexecutions') if r[3] == 'done'])).aggregate(0, lambda a, x: x, lambda a, x: a + x).run()")[0][0])

        for i in range(20):
            conn.delete(str(i))

        res = 0
        while res < 80:
            res = int(env.cmd('rg.pyexecute', "GB('ShardsIDReader').map(lambda x: len([r for r in execute('rg.dumpexecutions') if r[3] == 'done'])).aggregate(0, lambda a, x: x, lambda a, x: a + x).run()")[0][0])

        # wait for all executions to finish

        numOfKeys = env.cmd('rg.pyexecute', "GB().map(lambda x: int(x['value'])).aggregate(0, lambda a, x: x, lambda a, x: a + x).run('NumOfKeys*')")[0][0]
        env.assertEqual(numOfKeys, '0')

# @jvmTestDecorator()
# def testRegistersReplicatedToSlave():
#     env = Env(useSlaves=True, env='oss')
#     if env.envRunner.debugger is not None:
#         env.skip() # valgrind is not working correctly with replication
#     conn = getConnectionByEnv(env)
#     env.cmd('rg.pyexecute', "GB().filter(lambda x: x['key'] != 'NumOfKeys')."
#                             "foreach(lambda x: execute('incrby', 'NumOfKeys', ('1' if 'value' in x.keys() else '-1')))."
#                             "register()")

#     time.sleep(0.5) # wait for registration to reach all the shards

#     slaveConn = env.getSlaveConnection()
#     try:
#         with TimeLimit(5):
#             res = []
#             while len(res) < 1:
#                 res = slaveConn.execute_command('RG.DUMPREGISTRATIONS')
#     except Exception:
#         env.assertTrue(False, message='Failed waiting for Execution to reach slave')

#     for i in range(5):
#         conn.set(str(i), str(i))

#     try:
#         with TimeLimit(5):
#             numOfKeys = '0'
#             while numOfKeys != '5':
#                 numOfKeys = conn.get('NumOfKeys')
#     except Exception:
#         env.assertTrue(False, message='Failed waiting for keys to update')
    

#     ## make sure registrations did not run on slave (if it did NumOfKeys would get to 200)
#     try:
#         with TimeLimit(5):
#             numOfKeys = '0'
#             while numOfKeys != '5':
#                 numOfKeys = slaveConn.get('NumOfKeys')
#     except Exception:
#         env.assertTrue(False, message='Failed waiting for keys to update')

#     ## make sure registrations did not run on slave (if it did NumOfKeys would get to 200)
#     try:
#         with TimeLimit(5):
#             done = False
#             while not done:
#                 done = True
#                 executions = env.cmd('RG.DUMPEXECUTIONS')
#                 for r in executions:
#                     try:
#                         env.cmd('RG.DROPEXECUTION', r[1])
#                     except Exception:
#                         done = False
#     except Exception:
#         env.assertTrue(False, message='Failed dropping all the executions')

#     registrations = env.cmd('RG.DUMPREGISTRATIONS')
#     for r in registrations:
#          env.expect('RG.UNREGISTER', r[1]).equal('OK')

#     try:
#         with TimeLimit(5):
#             res = slaveConn.execute_command('RG.DUMPREGISTRATIONS')
#             while len(res) > 0:
#                 res = slaveConn.execute_command('RG.DUMPREGISTRATIONS')
#     except Exception:
#         env.assertTrue(False, message='Failed waiting for registration to unregister on slave')