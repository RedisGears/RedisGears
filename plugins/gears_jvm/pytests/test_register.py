from common import jvmTestDecorator
from common import putKeys
from common import TimeLimit
import time

@jvmTestDecorator()
def testBasicRegistration(env, conn, **kargs):
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
def testBasicStreamRegistration(env, conn, **kargs):
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
def testRegistersOnPrefix(env, conn, **kargs):
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
def testRegistersSurviveRestart(env, conn, **kargs):
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

@jvmTestDecorator(envArgs={'useSlaves': True, 'env': 'oss'})
def testRegistersReplicatedToSlave(env, conn, **kargs):
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication
    # env.cmd('rg.pyexecute', "GB().filter(lambda x: x['key'] != 'NumOfKeys')."
    #                         "foreach(lambda x: execute('incrby', 'NumOfKeys', ('1' if 'value' in x.keys() else '-1')))."
    #                         "register()")

    slaveConn = env.getSlaveConnection()
    try:
        with TimeLimit(5):
            res = []
            while len(res) < 1:
                res = slaveConn.execute_command('RG.DUMPREGISTRATIONS')
    except Exception:
        env.assertTrue(False, message='Failed waiting for Execution to reach slave')

    for i in range(5):
        conn.set(str(i), str(i))

    try:
        with TimeLimit(5):
            numOfKeys = '0'
            while numOfKeys != '5':
                numOfKeys = conn.get('NumOfKeys')
    except Exception:
        env.assertTrue(False, message='Failed waiting for keys to update')
    

    ## make sure registrations did not run on slave (if it did NumOfKeys would get to 200)
    try:
        with TimeLimit(5):
            numOfKeys = '0'
            while numOfKeys != '5':
                numOfKeys = slaveConn.get('NumOfKeys')
    except Exception:
        env.assertTrue(False, message='Failed waiting for keys to update')

    ## make sure registrations did not run on slave (if it did NumOfKeys would get to 200)
    try:
        with TimeLimit(5):
            done = False
            while not done:
                done = True
                executions = env.cmd('RG.DUMPEXECUTIONS')
                for r in executions:
                    try:
                        env.cmd('RG.DROPEXECUTION', r[1])
                    except Exception:
                        done = False
    except Exception:
        env.assertTrue(False, message='Failed dropping all the executions')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

    try:
        with TimeLimit(5):
            res = slaveConn.execute_command('RG.DUMPREGISTRATIONS')
            while len(res) > 0:
                res = slaveConn.execute_command('RG.DUMPREGISTRATIONS')
    except Exception:
        env.assertTrue(False, message='Failed waiting for registration to unregister on slave')

@jvmTestDecorator()
def testSyncRegister(env, conn, **kargs):
    env.skipOnCluster()

    for i in range(100):
        conn.set(str(i), str(i))

    env.assertEqual(conn.get('NumOfKeys'), '100')
    
@jvmTestDecorator()
def testOnRegisteredCallback(env, results, errs, executionError, **kargs):
    env.expect('rg.pyexecute', "GB().map(lambda x: x['value']).collect().distinct().run('registered*')").equal([['1'], []])

@jvmTestDecorator()
def testStreamReaderTrimming(env, **kargs):
    env.skipOnCluster()

    for i in range(3):
        env.execute_command('xadd', 'stream', '*', 'foo', 'bar')

    try:
        with TimeLimit(5):
            num = 0
            while num is None or int(num) != 3:
                num = env.execute_command('get', 'NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 3')

    env.expect('XLEN', 'stream').equal(0)

@jvmTestDecorator()
def testKeysReaderEventTypeFilter(env, conn, **kargs):
    conn.lpush('l', '1')
    conn.rpush('l', '1')

    try:
        with TimeLimit(10):
            counter = 0
            while counter is None or int(counter) != 2:
                counter = conn.get('counter')
                time.sleep(0.1)
    except Exception as e:
        print e
        env.assertTrue(False, message='Failed waiting for counter to reach 2')

    ## make sure other commands are not triggers executions
    conn.set('x', '1')

    try:
        with TimeLimit(2):
            counter = 0
            while counter is None or int(counter) != 3:
                counter = conn.get('counter')
                time.sleep(0.1)
            env.assertTrue(False, message='Counter reached 3 while not expected')
    except Exception as e:
        pass

@jvmTestDecorator()
def testKeysReaderKeyTypeFilter(env, conn, **kargs):
    conn.lpush('l', '1')
    conn.rpush('l', '1')

    try:
        with TimeLimit(10):
            counter = 0
            while counter is None or int(counter) != 2:
                counter = conn.get('counter')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for counter to reach 2')

    ## make sure other commands are not triggers executions
    conn.set('x', '1')

    try:
        with TimeLimit(2):
            counter = 0
            while counter is None or int(counter) != 3:
                counter = conn.get('counter')
                time.sleep(0.1)
            env.assertTrue(False, message='Counter reached 3 while not expected')
    except Exception as e:
        pass

@jvmTestDecorator()
def testCommandReaderBasic(env, **kargs):
    env.expect('RG.TRIGGER', 'test1', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test1', 'this'])
    env.expect('RG.TRIGGER', 'test2', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test2', 'this'])
    env.expect('RG.TRIGGER', 'test3', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test3', 'this'])

@jvmTestDecorator(preExecute=putKeys({'x':'foo', 'y':'bar', 'z':'foo'}))
def testUnregisterCallback(env, conn, **kargs):
    registrationId = env.cmd('RG.DUMPREGISTRATIONS')[0][1]
    env.cmd('RG.UNREGISTER', registrationId)
    env.expect('RG.PYEXECUTE', 'GB().count().run()').equal([[], []])
    

