from RLTest import Env, Defaults
import sys
import os
import time
from includes import *

from common import getConnectionByEnv
from common import TimeLimit
from common import verifyRegistrationIntegrity
from common import Background
from common import gearsTest

class testUnregister:
    def __init__(self):
        self.env = Env(freshEnv=True, decodeResponses=True)
        self.conn = getConnectionByEnv(self.env)

    def cleanUp(self):
        executions = self.env.cmd('RG.DUMPEXECUTIONS')
        for e in executions:
            self.env.cmd('RG.DROPEXECUTION', e[1])

        # delete all registrations so valgrind check will pass
        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        for r in registrations:
            self.env.expect('RG.UNREGISTER', r[1]).equal('OK')

    def testSimpleRegister(self):
        script = '''
GB().filter(lambda r: r['key'] != 'all_keys').repartition(lambda r: 'all_keys').foreach(lambda r: execute('sadd', 'all_keys', r['key'])).register()
        '''
        self.env.expect('RG.PYEXECUTE', script).equal('OK')
        time.sleep(1) # waiting for the execution to reach all shard, in the future we will use acks and return reply only
                      # when it reach all shards
        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        self.env.assertEqual(len(registrations), 1)
        registrationID = registrations[0][1]
        self.conn.execute_command('set', 'x', '1')
        time.sleep(1)
        res = self.conn.execute_command('smembers', 'all_keys')
        self.env.assertEqual(res.pop(), 'x')
        executions = self.env.cmd('RG.DUMPEXECUTIONS')
        for e in executions:
            self.env.assertEqual(1, e[5])

        self.cleanUp()

    def testSimpleUnregister(self):
        script = '''
GB().filter(lambda r: r['key'] != 'all_keys').repartition(lambda r: 'all_keys').foreach(lambda r: execute('sadd', 'all_keys', r['key'])).register()
        '''
        self.env.expect('RG.PYEXECUTE', script).equal('OK')
        time.sleep(1) # waiting for the execution to reach all shard, in the future we will use acks and return reply only
                      # when it reach all shards
        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        self.env.assertEqual(len(registrations), 1)
        registrationID = registrations[0][1]
        self.conn.execute_command('set', 'x', '1')
        time.sleep(1)
        res = self.conn.execute_command('smembers', 'all_keys')
        self.env.assertEqual(res.pop(), 'x')

        self.env.expect('RG.UNREGISTER', registrationID).equal('OK')
        time.sleep(1) # wait for dump registrations to reach all the shards

        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        self.env.assertEqual(len(registrations), 0)

        self.conn.execute_command('set', 'y', '1')
        time.sleep(1)
        res = self.conn.execute_command('smembers', 'all_keys')
        self.env.assertEqual(res.pop(), 'x')

        executions = self.env.cmd('RG.DUMPEXECUTIONS')
        for e in executions:
            self.env.cmd('RG.DROPEXECUTION', e[1])

        # delete all registrations so valgrind check will pass
        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        for r in registrations:
            self.env.expect('RG.UNREGISTER', r[1]).equal('OK')

        self.cleanUp()

    def testUnregisterWithStreamReader(self):
        res = self.env.cmd('rg.pyexecute', "GearsBuilder('StreamReader')."
                                      "map(lambda x: x['value'])."
                                      "flatmap(lambda x: [(a[0], a[1]) for a in x.items()])."
                                      "repartition(lambda x: x[0])."
                                      "foreach(lambda x: redisgears.executeCommand('set', x[0], x[1]))."
                                      "map(lambda x: str(x))."
                                      "register('stream1')", 'UNBLOCKING')
        self.env.assertEqual(res, 'OK')
        if(res != 'OK'):
            return

        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        self.env.assertEqual(len(registrations), 1)
        registrationID = registrations[0][1]

        time.sleep(1)  # make sure the execution reached to all shards
        self.conn.execute_command('XADD', 'stream1', '*', 'f1', 'v1', 'f2', 'v2')
        res = []
        while len(res) < 1:
            res = self.env.cmd('rg.dumpexecutions')
        for e in res:
            self.env.broadcast('rg.getresultsblocking', e[1])
            self.env.cmd('rg.dropexecution', e[1])
        with TimeLimit(5, self.env, 'Failed waiting for keys to be updated'):
            f1 = self.conn.get('f1')
            f2 = self.conn.get('f2')
            while f1 != 'v1' and f2 != 'v2':
                f1 = self.conn.get('f1')
                f2 = self.conn.get('f2')

        self.env.expect('RG.UNREGISTER', registrationID).equal('OK')
        time.sleep(1)  # make sure the unregister reached to all shards

        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        self.env.assertEqual(len(registrations), 0)

        self.conn.execute_command('XADD', 'stream1', '*', 'f3', 'v3', 'f4', 'v4')
        self.env.assertEqual(self.conn.get('f3'), None)
        self.env.assertEqual(self.conn.get('f4'), None)

        executions = self.env.cmd('RG.DUMPEXECUTIONS')
        for e in executions:
            self.env.cmd('RG.DROPEXECUTION', e[1])

        # delete all registrations so valgrind check will pass
        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        for r in registrations:
            self.env.expect('RG.UNREGISTER', r[1]).equal('OK')

        self.cleanUp()

@gearsTest(skipOnCluster=True)
def testMaxExecutionPerRegistrationStreamReader(env):
    env.cmd('RG.CONFIGSET', 'MaxExecutionsPerRegistration', 1)
    env.expect('rg.pyexecute', "GB('StreamReader').register('s')").ok()

    for i in range(20):
        env.cmd('xadd', 's', '*', 'foo', 'bar')

    try:
        with TimeLimit(2):
            currLen = 0
            while currLen != 1:
                currLen = len(env.cmd('rg.dumpexecutions'))
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions to be dropped')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
        env.expect('RG.UNREGISTER', r[1], ).equal('OK')

@gearsTest(skipOnCluster=True)
def testMaxExecutionPerRegistrationKeysReader(env):
    env.cmd('RG.CONFIGSET', 'MaxExecutionsPerRegistration', 1)
    env.expect('rg.pyexecute', "GB().register('*')").ok()

    for i in range(20):
        env.cmd('set', 'x%d' % i, '1')

    try:
        with TimeLimit(2):
            currLen = 0
            while currLen != 1:
                currLen = len(env.cmd('rg.dumpexecutions'))
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions to be dropped')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
        env.expect('RG.UNREGISTER', r[1], ).equal('OK')

@gearsTest(skipOnCluster=True, envArgs={'moduleArgs': 'executionThreads 1'})
def testUnregisterKeysReaderWithAbortExecutions(env):
    infinitScript = '''
counter = 0
def InfinitLoop(r):
    import time
    global counter
    counter+=1
    while counter > 3: # enter an infinit loop on the third time
        time.sleep(0.1)
    return r
GB().map(InfinitLoop).register('*', mode='async_local')
    '''
    env.expect('rg.pyexecute', infinitScript).ok()

    env.cmd('set', 'x', '1')
    env.cmd('set', 'y', '1')
    env.cmd('set', 'z', '1')
    env.cmd('set', 'l', '1') # infinit loop
    env.cmd('set', 'm', '1') # pending execution

    registrationInfo = env.cmd('RG.DUMPREGISTRATIONS')
    registrationId = registrationInfo[0][1]

    try:
        with TimeLimit(2):
            done = False
            while not done:
                registrationInfo = env.cmd('RG.DUMPREGISTRATIONS')
                if registrationInfo[0][7][3] == 5 and registrationInfo[0][7][5] == 3:
                    done = True
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions')

    # create another execution, make sure its pending
    eid = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run()', 'UNBLOCKING')

    executionsInfo = env.cmd('RG.DUMPEXECUTIONS')
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'done']), 3)
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'running']), 1)
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'created']), 2)

    env.expect('RG.UNREGISTER', registrationId, 'abortpending').ok()

    try:
        with TimeLimit(2):
            done = False
            while not done:
                exeutions = env.cmd('RG.DUMPEXECUTIONS')
                for e in exeutions:
                    # we need to abort the running execution, it was not aborted
                    # on RG.UNREGISTER
                    if e[3] == 'running':
                        try:
                            env.cmd('RG.ABORTEXECUTION', e[1])
                        except Exception:
                            pass
                if len(exeutions) == 1:
                    done = True
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for executions to be deleted')
    env.assertEqual(len(env.cmd('RG.DUMPEXECUTIONS')), 1)

    try:
        with TimeLimit(2):
            executionStatus = None
            while executionStatus != 'done':
                time.sleep(0.1)
                executionStatus = env.cmd('RG.GETEXECUTION', eid)[0][3][1]
    except Exception as e:
        env.assertTrue(False, message='Could not wait for execution to finish')

    env.cmd('RG.DROPEXECUTION', eid)

@gearsTest(skipOnCluster=True, envArgs={'moduleArgs': 'executionThreads 1'})
def testUnregisterStreamReaderWithAbortExecutions(env):
    infinitScript = '''
counter = 0
def InfinitLoop(r):
    import time
    global counter
    counter+=1
    while counter > 3: # enter an infinit loop on the third time
        time.sleep(0.1)
    return r
GB('StreamReader').map(InfinitLoop).register('s', mode='async_local', onFailedPolicy='abort')
    '''
    env.expect('rg.pyexecute', infinitScript).ok()

    env.cmd('xadd', 's', '*', 'foo', 'bar')

    # we have this part to make sure no two events will enter the same execution
    # because the first write triggers the background event that reads all the data 
    # from the stream.
    try:
        with TimeLimit(4):
            done = False
            while not done:
                registrationInfo = env.cmd('RG.DUMPREGISTRATIONS')
                if registrationInfo[0][7][3] == 2 and registrationInfo[0][7][5] == 2:
                    done = True
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions to finished')

    env.cmd('xadd', 's', '*', 'foo', 'bar')
    env.cmd('xadd', 's', '*', 'foo', 'bar')
    env.cmd('xadd', 's', '*', 'foo', 'bar') # infinit loop
    env.cmd('xadd', 's', '*', 'foo', 'bar') # pending execution

    registrationInfo = env.cmd('RG.DUMPREGISTRATIONS')
    registrationId = registrationInfo[0][1]

    try:
        with TimeLimit(4):
            done = False
            while not done:
                registrationInfo = env.cmd('RG.DUMPREGISTRATIONS')
                if registrationInfo[0][7][3] == 5 and registrationInfo[0][7][5] == 4:
                    done = True
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions')

    # create another execution, make sure its pending
    eid = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run()', 'UNBLOCKING')

    executionsInfo = env.cmd('RG.DUMPEXECUTIONS')
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'done']), 4)
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'running']), 1)
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'created']), 1)

    env.expect('RG.UNREGISTER', registrationId, 'abortpending').ok()

    try:
        with TimeLimit(2):
            while True:
                exeutions = env.cmd('RG.DUMPEXECUTIONS')
                for e in exeutions:
                    # we need to abort the running execution, it was not aborted
                    # on RG.UNREGISTER
                    if e[3] == 'running':
                        try:
                            env.cmd('RG.ABORTEXECUTION', e[1])
                        except Exception:
                            pass
                if len(exeutions) == 1:
                    break
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions to finished')

    try:
        with TimeLimit(2):
            executionStatus = None
            while executionStatus != 'done':
                time.sleep(0.1)
                executionStatus = env.cmd('RG.GETEXECUTION', eid)[0][3][1]
    except Exception as e:
        env.assertTrue(False, message='Could not wait for execution to finish')

    env.cmd('RG.DROPEXECUTION', eid)

@gearsTest()
def testBasicStream(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "GearsBuilder()."
                                  "filter(lambda x:x['key'] != 'values' and x['type'] != 'empty')."
                                  "repartition(lambda x: 'values')."
                                  "foreach(lambda x: redisgears.executeCommand('lpush', 'values', x['value']))."
                                  "register('*')", 'UNBLOCKING')
    env.assertEqual(res, 'OK')
    if(res != 'OK'):
        return
    verifyRegistrationIntegrity(env)
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

    # delete all registrations so valgrind check will pass
    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest()
def testBasicStreamRegisterOnPrefix(env):
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', "GearsBuilder('StreamReader')."
                               "map(lambda x: str(x['value']))."
                               "repartition(lambda x: 'new_key')."
                               "foreach(lambda x: redisgears.executeCommand('set', 'new_key', x))."
                               "register('s*')").ok()

    time.sleep(0.5)  # make sure the execution reached to all shards

    conn.execute_command('xadd', 'stream1', '*', 'name', 'test')
    res = []

    try:
        with TimeLimit(5):
            res = ''
            while res is None or "{'name': 'test'}" not in res:
                res = conn.get('new_key')
    except Exception:
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

    time.sleep(0.1) # waiting for all the execution to be fully created on all the shards

    # delete all registrations and executions so valgrind check will pass
    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
        env.broadcast('rg.getresultsblocking', r[1])
        env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
        env.expect('RG.UNREGISTER', r[1]).equal('OK')

    time.sleep(0.1) # wait for registration to unregister
    # todo: remove the need for this

@gearsTest()
def testBasicStreamProcessing(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "GearsBuilder('StreamReader')."
                                  "flatmap(lambda x: [(a[0], a[1]) for a in x['value'].items()])."
                                  "repartition(lambda x: x[0])."
                                  "foreach(lambda x: redisgears.executeCommand('set', x[0], x[1]))."
                                  "map(lambda x: str(x))."
                                  "register('stream1')", 'UNBLOCKING')
    env.assertEqual(res, 'OK')
    if(res != 'OK'):
        return
    time.sleep(0.5)  # make sure the registration reached to all shards
    env.cmd('XADD', 'stream1', '*', 'f1', 'v1', 'f2', 'v2')
    try:
        with TimeLimit(5):
            while True:
                if conn.get('f1') != 'v1':
                    continue
                if conn.get('f2') != 'v2':
                    continue
                break
    except Exception:
        env.assertTrue(False, message='Failed waiting for keys to updated')

@gearsTest()
def testRegistersOnPrefix(env):
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB()."
                            "filter(lambda x: x['type'] != 'empty')."
                            "map(lambda x: ('pref2:' + x['key'].split(':')[1], x['value']))."
                            "repartition(lambda x: x[0])."
                            "foreach(lambda x: execute('set', x[0], x[1]))."
                            "register(regex='pref1:*')")

    time.sleep(0.1) ## wait for registration to get to all shards

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

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest()
def testRegistersSurviveRestart(env):
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB().filter(lambda x: 'NumOfKeys' not in x['key'])."
                            "foreach(lambda x: execute('incrby', 'NumOfKeys{%s}' % (hashtag()), ('-1' if x['event'] == 'del' else '1')))."
                            "register(mode='async_local')")

    # todo: change it not to use sleep
    time.sleep(0.5) # wait for registration to reach all the shards

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


    # deleting all executions from all the shards, execution list are not identical so we use gears to clear it.
    res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').flatmap(lambda x: [r[1] for r in execute('rg.dumpexecutions')]).foreach(lambda x: execute('RG.DROPEXECUTION', x)).run()")

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
        env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest(envArgs={'useSlaves': True, 'env': 'oss'})
def testRegistersReplicatedToSlave(env):
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB().filter(lambda x: x['key'] != 'NumOfKeys')."
                            "foreach(lambda x: execute('incrby', 'NumOfKeys', ('1' if 'value' in x.keys() else '-1')))."
                            "register()")

    env.expect('wait', '1', '10000').equal(1)

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

@gearsTest(skipOnCluster=True)
def testSyncRegister(env):
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB().filter(lambda x: x['key'] != 'NumOfKeys')."
                            "foreach(lambda x: execute('incrby', 'NumOfKeys', ('1' if 'value' in x.keys() else '-1')))."
                            "register(mode='sync')")

    for i in range(100):
        conn.set(str(i), str(i))

    env.assertEqual(conn.get('NumOfKeys'), '100')

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest()
def testOnRegisteredCallback(env):
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB()."
                            "register(mode='async_local', onRegistered=lambda: execute('set', 'registered{%s}' % (hashtag()), '1'))")
    time.sleep(0.5) # make sure registered on all shards
    env.expect('rg.pyexecute', "GB().map(lambda x: x['value']).collect().distinct().run('registered*')").equal([['1'], []])

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest(skipOnCluster=True)
def testStreamReaderDoNotLoseValues(env):
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB('StreamReader')."
                            "foreach(lambda x: execute('incr', 'NumOfElements'))."
                            "register(regex='s', batch=5)")

    for i in range(5):
        conn.execute_command('xadd', 's', '*', 'foo', 'bar')

    try:
        with TimeLimit(10):
            num = 0
            while num is None or int(num) != 5:
                num = conn.get('NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 5')

    # lets add 4 more elements, no execution will be triggered.
    for i in range(4):
        conn.execute_command('xadd', 's', '*', 'foo', 'bar')

    env.dumpAndReload()

    try:
        with TimeLimit(5):
            num = 0
            while num is None or int(num) != 9:
                num = conn.get('NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 9')

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest(envArgs={'useAof': True, 'env': 'oss'})
def testStreamReaderWithAof(env):
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB('StreamReader').repartition(lambda x: 'NumOfElements')."
                            "foreach(lambda x: execute('incr', 'NumOfElements'))."
                            "register(regex='s', batch=5)")

    time.sleep(0.5) # wait for reach all shards

    for i in range(5):
        conn.execute_command('xadd', 's', '*', 'foo', 'bar')

    try:
        with TimeLimit(10):
            num = 0
            while num is None or int(num) != 5:
                num = conn.get('NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 5')

    # lets add 4 more elements, no execution will be triggered.
    for i in range(4):
        conn.execute_command('xadd', 's', '*', 'foo', 'bar')

    env.restartAndReload()

    # execution should be triggered on start for the rest of the elements
    # make sure it complited
    try:
        with TimeLimit(5):
            num = 0
            while num is None or int(num) != 9:
                num = conn.get('NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 9')

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest(skipOnCluster=True)
def testStreamReaderTrimming(env):
    env.cmd('rg.pyexecute', "GB('StreamReader')."
                            "foreach(lambda x: execute('incr', 'NumOfElements'))."
                            "register(regex='stream', batch=3)")

    for i in range(3):
        env.execute_command('xadd', 'stream', '*', 'foo', 'bar')

    try:
        with TimeLimit(10):
            num = 0
            while num is None or int(num) != 3:
                num = env.execute_command('get', 'NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 3')

    env.expect('XLEN', 'stream').equal(0)

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest(envArgs={'useSlaves': True, 'env': 'oss'})
def testStreamReaderRestartOnSlave(env):
    script = '''
import time
def FailedOnMaster(r):
    numSlaves = int(execute('info', 'replication').split('\\n')[2].split(':')[1])
    currNum = execute('get', 'NumOfElements')
    if currNum is not None:
        currNum = int(currNum)
    if currNum == 5 and numSlaves == 1:
        execute('set', 'inside_loop', '1')
        raise Exception('stop')
    execute('incr', 'NumOfElements')
GB('StreamReader').foreach(FailedOnMaster).register(regex='stream', batch=3, onFailedPolicy='abort')
'''
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication

    env.expect('wait', '1', '10000').equal(1)
    slaveConn = env.getSlaveConnection()
    masterConn = env.getConnection()
    env.cmd('rg.pyexecute', script)
    verifyRegistrationIntegrity(env)

    for i in range(3):
        env.execute_command('xadd', 'stream', '*', 'foo', 'bar')

    try:
        with TimeLimit(10):
            num = 0
            while num is None or int(num) != 3:
                num = env.execute_command('get', 'NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 3')

    for i in range(3):
        env.execute_command('xadd', 'stream', '*', 'foo', 'bar')

    try:
        with TimeLimit(10):
            num = 0
            while num is None or int(num) != 5:
                num = env.execute_command('get', 'NumOfElements')
                time.sleep(0.1)
            inside_loop = False
            while not inside_loop:
                inside_loop = env.execute_command('get', 'inside_loop') == '1'
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 5')

    slaveConn.execute_command('SLAVEOF', 'NO', 'ONE') # slave should become master here and continue the execution

    try:
        with TimeLimit(10):
            num = 0
            while num is None or int(num) != 8:
                num = slaveConn.execute_command('get', 'NumOfElements')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for NumOfElements to reach 8 on slave')

@gearsTest()
def testKeysReaderEventTypeFilter(env):
    conn = getConnectionByEnv(env)

    # count how many lpush and rpush happened
    env.cmd('rg.pyexecute', "GB().repartition(lambda x: 'counter')."
                            "filter(lambda x: 'value' in x.keys() and x['type'] != 'empty')."
                            "foreach(lambda x: execute('incr', 'counter'))."
                            "register(regex='*', eventTypes=['lpush', 'rpush'])")

    time.sleep(0.5) # wait for reach all shards

    conn.lpush('l', '1')
    conn.rpush('l', '1')

    try:
        with TimeLimit(10):
            counter = 0
            while counter is None or int(counter) != 2:
                counter = conn.get('counter')
                time.sleep(0.1)
    except Exception as e:
        print(e)
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

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest()
def testKeysReaderKeyTypeFilter(env):
    conn = getConnectionByEnv(env)

    # count how many lpush and rpush happened
    env.cmd('rg.pyexecute', "GB().repartition(lambda x: 'counter')."
                            "filter(lambda x: x['type'] != 'empty')."
                            "foreach(lambda x: execute('incr', 'counter'))."
                            "register(regex='*', keyTypes=['list'])")

    time.sleep(0.5) # wait for registration reach all shards

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

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest(skipOnCluster=True)
def testSteamReaderAbortOnFailure(env):

    # count how many lpush and rpush happened
    env.cmd('rg.pyexecute', "GB('StreamReader').foreach(lambda r: blalala)."
                            "register(regex='s', mode='async_local', onFailedPolicy='abort')")

    env.expect('xadd', 's', '*', 'foo', 'bar')
    env.expect('xadd', 's', '*', 'foo', 'bar')
    env.expect('xadd', 's', '*', 'foo', 'bar')


    try:
        with TimeLimit(2):
            while True:
                registrations = env.cmd('rg.DUMPREGISTRATIONS')
                if registrations[0][7][25] == 'ABORTED':
                    break
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for registration to abort')

    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest()
def testStreamTrimming(env):
    conn = getConnectionByEnv(env)

    # {06S} is going to first slot
    env.cmd('rg.pyexecute', "GB('StreamReader').register('s1{06S}', mode='async_local')")
    env.cmd('rg.pyexecute', "GB('StreamReader').register('s2{06S}', mode='async_local', trimStream=False)")

    time.sleep(0.5) # wait for registration to reach all the shards

    env.cmd('XADD s2{06S} * foo bar')
    env.cmd('XADD s1{06S} * foo bar')
    

    try:
        with TimeLimit(2):
            while True:
                len = env.cmd('XLEN s1{06S}')
                if int(len) == 0:
                    break
    except Exception as e:
        env.assertTrue(False, message='Could not wait for s1{06S} len to reach zero')

    env.assertEqual(env.cmd('XLEN s2{06S}'), 1)

    try:
        with TimeLimit(4):
            isDone = False
            while not isDone:
                isDone = True
                executions = env.cmd('RG.DUMPEXECUTIONS')
                for r in executions:
                    try:
                        res = env.cmd('RG.DROPEXECUTION', r[1])
                    except Exception:
                        res = 'error'
                    if res != 'OK':
                        isDone = False
                        time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not drop all executions')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

@gearsTest()
def testCommandReaderBasic(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x).distinct().sort().register(trigger='test1')").ok()
    env.expect('RG.TRIGGER', 'test1', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test1', 'this'])
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x).distinct().sort().register(trigger='test2', mode='sync')").ok()
    env.expect('RG.TRIGGER', 'test2', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test2', 'this'])
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x).distinct().sort().register(trigger='test3', mode='async_local')").ok()
    env.expect('RG.TRIGGER', 'test3', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test3', 'this'])

@gearsTest()
def testCommandReaderCluster(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='GetNumShard')").ok()
    env.expect('RG.TRIGGER', 'GetNumShard').equal([str(env.shardsCount)])

@gearsTest(skipOnCluster=True)
def testCommandReaderWithCountBy(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x[1:]).countby(lambda x: x).register(trigger='test1')").ok()
    env.expect('RG.TRIGGER', 'test1', 'a', 'a', 'a').equal(["{'key': 'a', 'value': 3}"])

    # we need to check twice to make sure the execution reset are not causing issues
    env.expect('RG.TRIGGER', 'test1', 'a', 'a', 'a').equal(["{'key': 'a', 'value': 3}"])

@gearsTest()
def testKeyReaderRegisterDontReadValues(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('KeysReader').foreach(lambda x: x.pop('event', None)).repartition(lambda x: 'l').foreach(lambda x: execute('lpush', 'l', str(x))).register(readValue=False, eventTypes=['set'])").ok()

    time.sleep(0.5) # make sure registration reached all shards

    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')


    try:
        with TimeLimit(4):
            while True:
                res = conn.execute_command('lrange', 'l', '0', '-1')
                if set(res) == set(["{'key': 'z'}", "{'key': 'y'}", "{'key': 'x'}"]):
                    break
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for list to popultae')

@gearsTest()
def testCommandOverrideHset(env):
    conn = getConnectionByEnv(env)
    script = '''
import time
def doHset(x):
    x += ['__time', time.time()]
    return call_next(*x[1:])
GB("CommandReader").map(doHset).register(hook="hset", mode="sync")
    '''
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('hset', 'h1', 'foo', 'bar')
    conn.execute_command('hset', 'h2', 'foo', 'bar')
    conn.execute_command('hset', 'h3', 'foo', 'bar')

    res = conn.execute_command('hget', 'h1', '__time')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h2', '__time')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h3', '__time')
    env.assertNotEqual(res, None)

@gearsTest()
def testCommandOverrideHsetMultipleTimes(env):
    conn = getConnectionByEnv(env)
    script1 = '''
import time
def doHset(x):
    x += ['__time1', time.time()]
    return call_next(*x[1:])
GB("CommandReader").map(doHset).register(hook="hset", mode="sync")
    '''
    env.expect('rg.pyexecute', script1).ok()

    script2 = '''
import time
def doHset(x):
    x += ['__time2', time.time()]
    return call_next(*x[1:])
GB("CommandReader").map(doHset).register(hook="hset", mode="sync")
    '''
    env.expect('rg.pyexecute', script2).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('hset', 'h1', 'foo', 'bar')
    conn.execute_command('hset', 'h2', 'foo', 'bar')
    conn.execute_command('hset', 'h3', 'foo', 'bar')

    res = conn.execute_command('hget', 'h1', '__time1')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h2', '__time1')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h3', '__time1')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h1', '__time2')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h2', '__time2')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h3', '__time2')
    env.assertNotEqual(res, None)

@gearsTest()
def testCommandOverrideHsetByKeyPrefix(env):
    conn = getConnectionByEnv(env)
    script1 = '''
import time
def doHset(x):
    x += ['__time1', time.time()]
    return call_next(*x[1:])
GB("CommandReader").map(doHset).register(hook="hset", mode="sync", keyprefix='h')
    '''
    env.expect('rg.pyexecute', script1).ok()

    script2 = '''
import time
def doHset(x):
    x += ['__time2', time.time()]
    return call_next(*x[1:])
GB("CommandReader").map(doHset).register(hook="hset", mode="sync", keyprefix='h', convertToStr=False)
    '''
    env.expect('rg.pyexecute', script2).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('hset', 'h1', 'foo', 'bar')
    conn.execute_command('hset', 'h2', 'foo', 'bar')
    conn.execute_command('hset', 'h3', 'foo', 'bar')
    conn.execute_command('hset', 'b1', 'foo', 'bar')

    res = conn.execute_command('hget', 'h1', '__time1')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h2', '__time1')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h3', '__time1')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h1', '__time2')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h2', '__time2')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'h3', '__time2')
    env.assertNotEqual(res, None)

    res = conn.execute_command('hget', 'b1', '__time1')
    env.assertEqual(res, None)

    res = conn.execute_command('hget', 'b1', '__time2')
    env.assertEqual(res, None)

@gearsTest()
def testAwaitOnAnotherExcecution(env):
    script = '''
async def doTest(x):
    res = await GB().map(lambda x: x['key']).run()
    if len(res[1]) > 0:
        raise Exception(res[1][0])
    return res[0]

GB("CommandReader").map(doTest).flatmap(lambda x: x).sort().register(trigger="test", mode="async_local")
    '''
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')

    env.expect('rg.trigger', 'test').equal(['x', 'y', 'z'])

@gearsTest(envArgs={'useSlaves': True, 'env': 'oss'})
def testHookNotTriggerOnReplicationLink(env):
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication
    script = '''
def doHset(x):
    execute('hincrby', x[1], '__updated', '1')
    return call_next(*x[1:])

GB("CommandReader").map(doHset).register(hook="hset", convertToStr=False)
    '''
    env.expect('wait', '1', '10000').equal(1)

    env.expect('rg.pyexecute', script).ok()

    env.expect('hset', 'h', 'foo', 'bar').equal(1)

    env.expect('hget', 'h', '__updated').equal('1')

    slaveConn = env.getSlaveConnection()

    try:
        with TimeLimit(4):
            while True:
                res = slaveConn.execute_command('hget', 'h', '__updated')
                if res == '1':
                    break
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for data to sync to slave')

@gearsTest()
def testMultipleRegistrationsSameBuilderWithKeysReader(env):
    script = '''
gb = GB().foreach(lambda x: execute('del', x['key']))
gb.register(prefix='foo', readValue=False, mode='sync')
gb.register(prefix='bar', readValue=False, mode='sync')
    '''
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('set', 'foo', '1')
    conn.execute_command('set', 'bar', '2')
    env.assertEqual(conn.execute_command('get', 'foo'), None)
    env.assertEqual(conn.execute_command('get', 'bar'), None)

@gearsTest()
def testMultipleRegistrationsSameBuilderWithStreamReader(env):
    script = '''
gb = GB('StreamReader').foreach(lambda x: execute('hset', '{%s}_hash' % x['key'], *sum([[k,v] for k,v in x['value'].items()], [])))
gb.register(prefix='foo', mode='sync')
gb.register(prefix='bar', mode='sync')
    '''
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('xadd', 'foo', '*', 'x', '1')
    conn.execute_command('xadd', 'bar', '*', 'x', '1')
    env.assertEqual(conn.execute_command('hgetall', '{foo}_hash'), {'x': '1'})
    env.assertEqual(conn.execute_command('hgetall', '{bar}_hash'), {'x': '1'})

@gearsTest()
def testMultipleRegistrationsSameBuilderWithCommandReader(env):
    script = '''
import time

gb = GB('CommandReader').foreach(lambda x: call_next(x[1], '_time', time.time(), *x[2:]))
gb.register(hook='hset', mode='sync')
gb.register(hook='hmset', mode='sync')
    '''
    conn = getConnectionByEnv(env)
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    conn.execute_command('hset', 'h1', 'x', '1')
    conn.execute_command('hmset', 'h2', 'x', '1')
    env.assertNotEqual(conn.execute_command('hget', 'h1', '_time'), None)
    env.assertNotEqual(conn.execute_command('hget', 'h2', '_time'), None)

@gearsTest(skipOnCluster=True)
def testDeleteStreamDurringRun(env):
    script = '''
import time

GB("StreamReader").foreach(lambda x: time.sleep(2)).register(prefix='s')
    '''
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    env.cmd('xadd', 's', '*', 'foo', 'bar')
    time.sleep(1)
    env.cmd('flushall')

    # make sure all executions are done without crashing
    registration = env.cmd('RG.DUMPREGISTRATIONS')[0]
    while registration[7][3] != registration[7][5]:
        time.sleep(0.1)
        registration = env.cmd('RG.DUMPREGISTRATIONS')[0]

class testKeysReaderWithCommands():
    def __init__(self):
        self.env = Env(decodeResponses=True)
        self.conn = getConnectionByEnv(self.env)

    def setUp(self):
        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        for r in registrations:
            self.env.expect('RG.UNREGISTER', r[1]).equal('OK')

    def testKeysReaderWithCommandsOption(self):
        script = '''
GB().foreach(lambda x: override_reply('key does not exists')).register(eventTypes=['keymiss'], commands=['get', 'mget'], mode='sync')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)
        
        res = self.conn.execute_command('get', 'x')
        self.env.assertEqual(res, 'key does not exists')

    def testKeysReaderWithCommandsOptionOnAsynLocalExecution(self):
        script = '''
GB().foreach(lambda x: override_reply('key does not exists')).register(eventTypes=['keymiss'], commands=['get', 'mget'], mode='async_local')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)
        
        res = self.conn.execute_command('get', 'x')
        self.env.assertEqual(res, 'key does not exists')

    def testKeysReaderWithCommandsOptionOnAsynExecution(self):
        script = '''
GB().foreach(lambda x: override_reply('key does not exists')).register(eventTypes=['keymiss'], commands=['get', 'mget'], mode='async')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)
        
        res = self.conn.execute_command('get', 'x')
        self.env.assertEqual(res, 'key does not exists')

    def testKeysReaderWithCommandsOptionOnAsynLocalExecutionOnMultiExec(self):
        self.env.skipOnCluster()
        script = '''
GB().foreach(lambda x: override_reply('key does not exists')).register(eventTypes=['keymiss'], commands=['get', 'mget'], mode='async_local')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)


        self.env.cmd('multi')
        self.env.cmd('get', 'x')
        self.env.expect('exec').equal([None])

    def testKeysReaderWithCommandsOptionOnSynExecutionOnMultiExec(self):
        self.env.skipOnCluster()
        script = '''
GB().foreach(lambda x: override_reply('key does not exists')).register(eventTypes=['keymiss'], commands=['get', 'mget'], mode='sync')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)


        self.env.cmd('multi')
        self.env.cmd('get', 'x')
        self.env.expect('exec').equal(['key does not exists'])

    def testKeysReaderWithCommandsOptionOnAsyncAwait(self):
        script = '''
async def OverrideReply(x):
    res = await GB('ShardsIDReader').run()
    override_reply(len(res))

GB().foreach(OverrideReply).register(eventTypes=['keymiss'], commands=['get', 'mget'], mode='async')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)
        
        res = self.conn.execute_command('get', 'x')
        self.env.assertEqual(res, 2)

    def testKeysReaderWithCommandsOptionWithKeyPrefix(self):
        script = '''
async def OverrideReply(x):
    res = await GB('ShardsIDReader').run()
    override_reply(len(res))

GB().foreach(OverrideReply).register(prefix='test*', eventTypes=['keymiss'], commands=['get', 'mget'], mode='async')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)
        
        res = self.conn.execute_command('get', 'x')
        self.env.assertEqual(res, None)

        res = self.conn.execute_command('get', 'test1')
        self.env.assertEqual(res, 2)

    def testKeysReaderWithCommandsUnregister(self):
        script = '''
async def OverrideReply(x):
    res = await GB('ShardsIDReader').run()
    override_reply(len(res))

GB().foreach(OverrideReply).register(prefix='test*', eventTypes=['keymiss'], commands=['get', 'mget'], mode='async')
        '''
        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)
        
        res = self.conn.execute_command('get', 'x')
        self.env.assertEqual(res, None)

        res = self.conn.execute_command('get', 'test1')
        self.env.assertEqual(res, 2)

        registrations = self.env.cmd('RG.DUMPREGISTRATIONS')
        for r in registrations:
            self.env.expect('RG.UNREGISTER', r[1]).equal('OK')

        res = self.conn.execute_command('get', 'test1')
        self.env.assertEqual(res, None)

        self.env.expect('rg.pyexecute', script).ok()

        verifyRegistrationIntegrity(self.env)

        res = self.conn.execute_command('get', 'test1')
        self.env.assertEqual(res, 2)

@gearsTest(skipOnCluster=True)
def testCommandHookWithExecute(env):
    script1 = '''
def my_hset(r):
    t = str(execute('time')[0])
    new_args = r[1:] + ['_last_modified_', t]
    return call_next(*new_args)

GB('CommandReader').map(my_hset).register(hook='hset', mode='sync')
    '''
    script2 = '''
def my_hset(r):
    execute('hincrby', r[1], '_times_modified_', 1)
    return call_next(*r[1:])

GB('CommandReader').map(my_hset).register(hook='hset', mode='sync')
    '''

    env.expect('rg.pyexecute', script1).ok()
    env.expect('rg.pyexecute', script2).ok()

    verifyRegistrationIntegrity(env)

    env.expect('hset', 'k1', 'foo', 'bar').equal('2')
    env.expect('hget', 'k1', '_times_modified_').equal('1')
    env.expect('HEXISTS', 'k1', '_last_modified_').equal(True)

@gearsTest(skipOnCluster=True)
def testCommandReaderInOrder(env):
    script = '''
import time

def SleepIfNeeded(x):
    if x[2] == 'x':
        time.sleep(0.5)

GB('CommandReader').foreach(SleepIfNeeded).map(lambda x: execute('lpush', x[1], x[2])).register(trigger='test_inorder', mode='async_local', inorder=True)

GB('CommandReader').foreach(SleepIfNeeded).map(lambda x: execute('lpush', x[1], x[2])).register(trigger='test_not_inorder', mode='async_local')
    '''
    env.expect('rg.pyexecute', script).ok()

    verifyRegistrationIntegrity(env)

    for _ in env.reloading_iterator():
        conn1 = env.getConnection()
        conn2 = env.getConnection()

        def RunTestNotInOrderX():
            conn1.execute_command('RG.TRIGGER', 'test_not_inorder', 'l', 'x')

        def RunTestNotInOrderY():
            conn2.execute_command('RG.TRIGGER', 'test_not_inorder', 'l', 'y')

        try:
            with Background(RunTestNotInOrderX) as bk1:
                time.sleep(0.1) # make sure X is sent first
                with Background(RunTestNotInOrderY) as bk2:
                    with TimeLimit(50):
                        while bk1.isAlive or bk2.isAlive:
                            time.sleep(0.1)
        except Exception as e:
            env.assertTrue(False, message='Failed wait for RunTestNotInOrder to finish: %s' % str(e))

        env.expect('lrange', 'l', '0', '-1').equal(['x', 'y'])

        env.cmd('flushall')

        def RunTestInOrderX():
            conn1.execute_command('RG.TRIGGER', 'test_inorder', 'l', 'x')

        def RunTestInOrderY():
            conn2.execute_command('RG.TRIGGER', 'test_inorder', 'l', 'y')

        try:
            with Background(RunTestInOrderX) as bk1:
                time.sleep(0.1) # make sure X is sent first
                with Background(RunTestInOrderY) as bk2:
                    with TimeLimit(50):
                        while bk1.isAlive or bk2.isAlive:
                            time.sleep(0.1)
        except Exception as e:  
            env.assertTrue(False, message='Failed wait for RunTestInOrder to finish: %s' % str(e))

        env.expect('lrange', 'l', '0', '-1').equal(['y', 'x'])

        env.cmd('flushall')

@gearsTest(skipOnCluster=True)
def testStreamReaderNotTriggerEventsOnReplica(env):
    env.expect('RG.PYEXECUTE', "GB('StreamReader').map(lambda x: x['error']).register(onFailedPolicy='retry', onFailedRetryInterval=1)").equal('OK')
    env.expect('xadd', 's', '*', 'foo', 'bar')
    
    res1 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    time.sleep(2)
    res2 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    env.assertTrue(res1 < res2)
    
    env.cmd('SLAVEOF', 'localhost', '22222')
    time.sleep(2)
    res1 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    time.sleep(2)
    res2 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    env.assertEqual(res1, res2)

    env.cmd('SLAVEOF', 'no', 'one')
    time.sleep(2)
    res1 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    time.sleep(2)
    res2 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    env.assertLess(res1, res2)

    env.cmd('SLAVEOF', 'localhost', '22222')
    time.sleep(2)
    res1 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    time.sleep(2)
    res2 = env.cmd('RG.DUMPREGISTRATIONS')[0][7][3]
    env.assertEqual(res1, res2)

    env.cmd('SLAVEOF', 'no', 'one')

@gearsTest(skipOnCluster=True)
def testMOD1960(env):
    env.cmd('xadd', 's', '*', 'foo', 'bar')
    env.expect('rg.pyexecute', "GB('StreamReader').foreach(lambda x: execute('set', 'x', '1')).register(mode='sync')").ok()
    try:
        with TimeLimit(4):
            while True:
                res = env.cmd('get', 'x')
                if res == '1':
                    break
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for x to be updated')

# test is no longer relevant, register execution will not start and nothing will registered
# keep it so maybe in the future we will have a better was to test it
# @gearsTest(skipCleanups=True, skipCallback=lambda: Defaults.num_shards != 3)
# def testStreamReaderOnUninitializedCluster(env):
#     conn = getConnectionByEnv(env)

#     # we know that s3 goes to the second shard
#     conn.execute_command('xadd', 's3', '*', 'foo', 'bar')

#     env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '100')

#     conn1 = env.getConnection(shardId=1)
#     conn2 = env.getConnection(shardId=2)

#     # close shard 1 to get cluster to a down state
#     env.envRunner.shards[0].stopEnv()

#     try:
#         with TimeLimit(1):
#             while True:
#                 res = conn2.execute_command('CLUSTER', 'INFO')
#                 if 'cluster_state:fail' in str(res):
#                     break
#                 time.sleep(0.1)
#     except Exception as e:
#         env.assertTrue(False, message='Failed waiting for down cluster state (%s)' % (str(e)))

#     print(conn2.execute_command('RG.PYEXECUTE', "GB('StreamReader').register()"))

#     # make sure no executions are created
#     try:
#         with TimeLimit(1):
#             while True:
#                 res = conn2.execute_command('RG.DUMPEXECUTIONS')
#                 if len(res) != 0:
#                     print(res)
#                     env.assertEqual(len(res), 0)
#                     break
#     except Exception as e:
#         pass

#     # restart the shard
#     env.envRunner.shards[0].startEnv()
#     conn1.execute_command('RG.REFRESHCLUSTER')

#     # make sure execution is eventually created
#     try:
#         with TimeLimit(5):
#             while True:
#                 res = conn2.execute_command('RG.DUMPEXECUTIONS')
#                 if len(res) >= 1:
#                     break
#                 time.sleep(0.1)
#     except Exception as e:
#         raw_input('stopped')
#         env.assertTrue(False, message='Failed waiting for execution to start (%s)' % (str(e)))

@gearsTest(skipCallback=lambda: Defaults.num_shards != 2)
def testMissEventOnClusterKeepsClusterErrors(env):
    env.expect('RG.PYEXECUTE', "GB().register(commands=['get'], eventTypes=['keymiss'])").equal('OK')
    verifyRegistrationIntegrity(env)

    conn1 = env.getConnection(shardId=1)

    try:
        conn1.execute_command('get', 'x')
    except Exception as e:
        env.assertContains('MOVED', str(e))

@gearsTest(skipCallback=lambda: Defaults.num_shards != 2)
def testCommandHookOnClusterKeepsClusterErrors(env):
    env.expect('RG.PYEXECUTE', "GB('CommandReader').register(hook='get', mode='sync')").equal('OK')
    verifyRegistrationIntegrity(env)

    conn1 = env.getConnection(shardId=1)

    try:
        conn1.execute_command('get', 'x')
        env.assertTrue(False, message='No error raised')
    except Exception as e:
        env.assertContains('MOVED', str(e))

@gearsTest(skipCallback=lambda: Defaults.num_shards != 2)
def testRGTriggerOnKey(env):
    conn2 = env.getConnection(shardId=2)
    env.expect('rg.pyexecute', "GB('CommandReader').foreach(lambda x: print('fooooooooooooo')).map(lambda x: execute('set', x[1], x[2])).register(trigger='my_set', mode='sync')").ok()
    verifyRegistrationIntegrity(env)

    env.expect('RG.TRIGGERONKEY', 'my_set', 'x', '1').error().contains('')
    conn2.execute_command('RG.TRIGGERONKEY', 'my_set', 'x', '1')

    env.assertEqual(conn2.execute_command('get', 'x'), '1')

@gearsTest(skipOnCluster=True)
def testGlobalsDictionaryOnDeserialization(env):
    script = '''
g = 1
def f1():
    global g
    g = g + 1
    print(id(globals()))
    return g

def f(x):
    global g
    g = g + 1
    print(id(globals()))
    return f1()

GB('CommandReader').map(f).register(trigger='test', convertToStr=False)
GB('CommandReader').map(f).register(trigger='test1', convertToStr=False)
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    for _ in env.reloading_iterator():
        env.expect('RG.TRIGGER', 'test').equal([3])
        env.expect('RG.TRIGGER', 'test1').equal([5])

@gearsTest()
def testCaseInsensetiveEventTypes(env):
    env.expect('rg.pyexecute', "GB().foreach(lambda x: execute('set', '{%s}1' % x['key'], '1')).register(eventTypes=['SET'], mode='sync')").ok()
    verifyRegistrationIntegrity(env)

    conn = getConnectionByEnv(env)

    conn.execute_command('set', 'x', '1')
    env.assertEqual(conn.execute_command('get', '{x}1'), '1')

@gearsTest(skipOnCluster=True)
def testRegistrationRunDurationOnKeysReader(env):
    script = '''
import time
def test(x):
    time.sleep(0.01)

GB('KeysReader').foreach(test).register(mode='sync')
    '''
    env.expect('rg.pyexecute', script).ok()

    env.execute_command('set', 'x', '1')

    res = env.cmd('RG.DUMPREGISTRATIONS')

    env.assertContains('lastRunDurationMS', res[0][7])
    env.assertContains('totalRunDurationMS', res[0][7])
    env.assertContains('avgRunDurationMS', res[0][7])


@gearsTest(skipOnCluster=True)
def testRegistrationRunDurationOnStreamReader(env):
    script = '''
import time
def test(x):
    time.sleep(0.01)

GB('StreamReader').foreach(test).register(mode='sync')
    '''
    env.expect('rg.pyexecute', script).ok()

    env.execute_command('xadd', 'x', '*', 'foo', 'bar')
    
    res = env.cmd('RG.DUMPREGISTRATIONS')
    
    env.assertContains('lastRunDurationMS', res[0][7])
    env.assertContains('totalRunDurationMS', res[0][7])
    env.assertContains('avgRunDurationMS', res[0][7])
    env.assertContains('lastEstimatedLagMS', res[0][7])
    env.assertContains('avgEstimatedLagMS', res[0][7])

@gearsTest(skipOnCluster=True)
def testRegistrationRunDurationOnCommandReader(env):
    script = '''
import time
def test(x):
    time.sleep(0.01)

GB('CommandReader').foreach(test).register(trigger='test', mode='sync')
    '''
    env.expect('rg.pyexecute', script).ok()

    env.execute_command('RG.TRIGGER', 'test')

    res = env.cmd('RG.DUMPREGISTRATIONS')

    env.assertContains('lastRunDurationMS', res[0][7])
    env.assertContains('totalRunDurationMS', res[0][7])
    env.assertContains('avgRunDurationMS', res[0][7])


@gearsTest()
def testRegistrationClearStats(env):
    script = '''
import time
def test(x):
    time.sleep(0.01)

GB('KeysReader').foreach(test).register(mode='sync')
GB('CommandReader').foreach(test).register(trigger='test', mode='sync')
GB('StreamReader').foreach(test).register(mode='sync')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    conn = getConnectionByEnv(env)

    conn.execute_command('set', 'x', '1')
    conn.execute_command('xadd', 'y', '*', 'foo', 'bar')
    env.execute_command('RG.TRIGGER', 'test')

    env.expect('RG.CLEARREGISTRATIONSSTATS').equal('OK')

    for i in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId=i)
        res = env.execute_command('RG.DUMPREGISTRATIONS')
        for r in res:
            d = {}
            r = r[7]
            for i in range(0, len(r), 2):
                d[r[i]] = r[i + 1]
            env.assertEqual(d['numTriggered'], 0)
            env.assertEqual(d['numSuccess'], 0)
            env.assertEqual(d['numFailures'], 0)
            env.assertEqual(d['numAborted'], 0)
            env.assertEqual(d['lastRunDurationMS'], 0)
            env.assertContains('0', d['avgRunDurationMS'])
            env.assertEqual(d['lastError'], None)
            if 'lastEstimatedLagMS' in d.keys():
                env.assertEqual(d['lastEstimatedLagMS'], 0)
            if 'avgEstimatedLagMS' in d.keys():
                env.assertContains('0', d['avgEstimatedLagMS'])
            
@gearsTest()
def testPauseUnpause(env):
    script = '''
def test(x):
    execute('incr', 'counter{%s}' % (x['key']))

regId = GB('StreamReader').foreach(test).register(mode='sync')
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    conn = getConnectionByEnv(env)

    conn.execute_command('xadd', 'x', '*', 'foo', 'bar')
    conn.execute_command('xadd', 'y', '*', 'foo', 'bar')

    counterX = conn.execute_command('get', 'counter{x}')
    counterY = conn.execute_command('get', 'counter{y}')
    env.assertEqual(counterX, '1')
    env.assertEqual(counterY, '1')

    env.execute_command('RG.PAUSEREGISTRATIONS', regId)

    conn.execute_command('xadd', 'x', '*', 'foo', 'bar')
    conn.execute_command('xadd', 'y', '*', 'foo', 'bar')

    counterX = conn.execute_command('get', 'counter{x}')
    counterY = conn.execute_command('get', 'counter{y}')
    env.assertEqual(counterX, '1')
    env.assertEqual(counterY, '1')

    env.execute_command('RG.UNPAUSEREGISTRATIONS', regId)

    with TimeLimit(2, env, 'Failed waiting for registration to unpaused'):
        while True:
            counterX = conn.execute_command('get', 'counter{x}')
            counterY = conn.execute_command('get', 'counter{y}')
            if counterX == '2' and counterY == '2':
                break
            time.sleep(0.1)

@gearsTest()
def testPauseUnpauseNotExistingID(env):
    env.expect('RG.PAUSEREGISTRATIONS', 'not_exists').error().contains('Execution not_exists does not exists on')
    env.expect('RG.UNPAUSEREGISTRATIONS', 'not_exists').error().contains('Execution not_exists does not exists on')

@gearsTest()
def testPauseUnpauseNotSupported(env):
    script = '''
regId = GB().register(mode='sync')
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]
    env.expect('RG.PAUSEREGISTRATIONS', regId).error().contains('Reader KeysReader does not support pause')
    env.expect('RG.UNPAUSEREGISTRATIONS', regId).error().contains('Reader KeysReader does not support unpause')

@gearsTest()
def testPauseFromWithinTheRegistrationCode(env):
    script = '''
def foreachFunc(x):
    isPaused = execute('get', 'paused{%s}' % (x['key']))
    if isPaused == 'yes':
        flat_error('PAUSE')
    execute('incr', 'counter{%s}' % (x['key']))

regId = GB('StreamReader').foreach(foreachFunc).register(mode='sync')
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    conn = getConnectionByEnv(env)

    conn.execute_command('xadd', 'x', '*', 'foo', 'bar')

    counterX = conn.execute_command('get', 'counter{x}')
    env.assertEqual(counterX, '1')

    conn.execute_command('set', 'paused{x}', 'yes')

    conn.execute_command('xadd', 'x', '*', 'foo', 'bar')

    counterX = conn.execute_command('get', 'counter{x}')
    env.assertEqual(counterX, '1')

    conn.execute_command('set', 'paused{x}', 'no')

    env.execute_command('RG.UNPAUSEREGISTRATIONS', regId)

    with TimeLimit(2, env, 'Failed waiting for registration to unpaused'):
        while True:
            counterX = conn.execute_command('get', 'counter{x}')
            if counterX == '2':
                break
            time.sleep(0.1)

@gearsTest()
def testStreamReaderDuration(env):
    script = '''
GB('StreamReader').repartition(lambda x: 'counter').map(lambda x: execute('incr', 'counter')).register(prefix='s1*', batch=2, duration=1)
GB('StreamReader').repartition(lambda x: 'counter').map(lambda x: execute('incr', 'counter')).register(prefix='s2*', batch=2, duration=1000)
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    conn = getConnectionByEnv(env)

    conn.execute_command('xadd', 's1', '*', 'foo', 'bar')
    conn.execute_command('xadd', 's1', '*', 'foo', 'bar')
    conn.execute_command('xadd', 's1', '*', 'foo', 'bar')

    with TimeLimit(2, env, 'Failed waiting for counter to reach 3'):
        while True:
            counter = conn.execute_command('get', 'counter')
            if counter == '3':
                break
            time.sleep(0.1)

    conn.execute_command('xadd', 's2', '*', 'foo', 'bar')
    conn.execute_command('xadd', 's2', '*', 'foo', 'bar')
    conn.execute_command('xadd', 's2', '*', 'foo', 'bar')

    with TimeLimit(2, env, 'Failed waiting for counter to reach 3'):
        while True:
            counter = conn.execute_command('get', 'counter')
            if counter == '6':
                break
            time.sleep(0.1)

@gearsTest()
def testStreamReaderOnTimmerPending(env):
    script = '''
regId = GB('StreamReader').repartition(lambda x: 'counter').map(lambda x: execute('incr', 'counter')).register(prefix='s2*', batch=2, duration=1000)
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    conn = getConnectionByEnv(env)

    conn.execute_command('xadd', 's2', '*', 'foo', 'bar')
    conn.execute_command('xadd', 's2', '*', 'foo', 'bar')
    conn.execute_command('xadd', 's2', '*', 'foo', 'bar')

    # here we have a pending timmer, lets unregister the registration.

    env.expect('RG.UNREGISTER', regId).equal('OK')

    time.sleep(1)

    # make sure cluster is still up
    for i in range(1, env.shardsCount + 1, 1):
        c = env.getConnection(i)
        res = c.execute_command('ping')
        env.assertEqual(res, True)

@gearsTest(skipOnCluster=True)
def testStreamReaderHoldFinishCurrentBatch(env):
    script = '''
import time
regId = GB('StreamReader').foreach(lambda x: execute('incr', 'x')).foreach(lambda x: time.sleep(2)).register()
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    env.expect('xadd', 's', '*', 'foo', 'bar')
    env.expect('xadd', 's', '*', 'foo', 'bar')

    with TimeLimit(5, env, 'Failed waiting for registration to start processing the stream'):
        while True:
            val = env.execute_command('get', 'x')
            if val == '1':
                break
            time.sleep(0.1)

    env.expect('RG.PAUSEREGISTRATIONS', regId).ok()

    with TimeLimit(5, env, 'Failed waiting stream len to be 1'):
        while True:
            size = env.execute_command('xlen', 's')
            if size == 1:
                break
            time.sleep(0.1)

@gearsTest(skipOnCluster=True)
def testStreamReaderUnregisterFinishesOnlyCurrentBatch(env):
    script = '''
import time
regId = GB('StreamReader').foreach(lambda x: execute('incr', 'x')).foreach(lambda x: time.sleep(2)).register()
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    env.expect('xadd', 's', '*', 'foo', 'bar')
    env.expect('xadd', 's', '*', 'foo', 'bar')

    with TimeLimit(5, env, 'Failed waiting for registration to start processing the stream'):
        while True:
            val = env.execute_command('get', 'x')
            if val == '1':
                break
            time.sleep(0.1)

    env.expect('RG.UNREGISTER', regId).ok()

    with TimeLimit(5, env, 'Failed waiting stream len to be 1'):
        while True:
            size = env.execute_command('xlen', 's')
            if size == 1:
                break
            time.sleep(0.1)

    # make sure the second element are not consumed
    try:
        with TimeLimit(4):
            while True:
                size = env.execute_command('xlen', 's')
                if size == 0:
                    env.assertTrue(False, message='Second element of the stream was consumed')
                time.sleep(0.1)
    except Exception as e:
        pass


@gearsTest(skipOnCluster=True)
def testFastPauseAndUnpause(env):
    script = '''
import time
regId = GB('StreamReader').foreach(lambda x: execute('incr', 'x')).foreach(lambda x: time.sleep(1)).register()
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    env.expect('xadd', 's', '*', 'foo', 'bar')
    env.expect('xadd', 's', '*', 'foo', 'bar')

    with TimeLimit(5, env, 'Failed waiting for registration to start processing the stream'):
        while True:
            val = env.execute_command('get', 'x')
            if val == '1':
                break
            time.sleep(0.1)

    env.expect('RG.PAUSEREGISTRATIONS', regId).ok()
    env.expect('RG.UNPAUSEREGISTRATIONS', regId).ok()

    # make sure the second element are not consumed
    try:
        with TimeLimit(1):
            while True:
                size = env.execute_command('xlen', 's')
                if size >= 2:
                    break
                time.sleep(0.1)
    except Exception as e:
        pass

@gearsTest(skipOnCluster=True)
def testStreamReaderPauseFailedRegistration(env):
    script = '''
import time
regId = GB('StreamReader').foreach(lambda x: execute('incr', 'x')).foreach(lambda x: foo()).register(onFailedPolicy='retry')
GB('CommandReader').map(lambda x: regId).register(mode='sync', trigger='get_reg_id')
    '''
    env.expect('rg.pyexecute', script).ok()
    verifyRegistrationIntegrity(env)

    regId = env.execute_command('RG.TRIGGER', 'get_reg_id')[0]

    env.cmd('xadd', 's', '*', 'foo', 'bar')

    env.expect('RG.PAUSEREGISTRATIONS', regId).ok()

    with TimeLimit(5, env, 'Failed waiting for registration to pause'):
        while True:
            registrations = env.cmd('RG.DUMPREGISTRATIONS')
            stream_registration = [r for r in registrations if r[3] == 'StreamReader'][0]
            if stream_registration[7][25] == 'PAUSED':
                break
            time.sleep(0.1)

@gearsTest(skipOnCluster=True)
def testStreamReaderInfinitLoopOnTrimmingDisabled(env):
    script = '''
import time
GB('StreamReader').foreach(lambda x: execute('incr', 'x')).register(batch=2, onFailedPolicy='retry', trimStream=False)
    '''
    env.expect('RG.PYEXECUTE', script, 'ID', 'test').ok()
    verifyRegistrationIntegrity(env)

    env.cmd('xadd', 's', '*', 'foo', 'bar')
    env.cmd('xadd', 's', '*', 'foo', 'bar')

    with TimeLimit(5, env, 'Failed waiting for registration to pause'):
        while True:
            registrations = env.cmd('RG.DUMPREGISTRATIONS')
            if registrations[0][7][5] == 2:
                break
            time.sleep(0.1)

    env.expect('RG.PYEXECUTE', script, 'ID', 'test', 'UPGRADE').ok()
    verifyRegistrationIntegrity(env)
    
    try:
        with TimeLimit(1):
            while True:
                registrations = env.cmd('RG.DUMPREGISTRATIONS')
                if registrations[0][7][3] > 2:
                    env.assertTrue(False, message='More than 2 executions were triggered')
                    break
                time.sleep(0.1)
    except Exception as e:
        if 'timeout' not in str(e):
            sys.stderr.write('%s\n' % str(e))
        env.assertContains('timeout', str(e))

    env.cmd('xadd', 's', '*', 'foo', 'bar')
    env.cmd('xadd', 's', '*', 'foo', 'bar')

    try:
        with TimeLimit(1):
            while True:
                registrations = env.cmd('RG.DUMPREGISTRATIONS')
                if registrations[0][7][3] != 3:
                    env.assertTrue(False, message='More than 3 executions were triggered')
                    break
                time.sleep(0.1)
    except Exception as e:
        if 'timeout' not in str(e):
            sys.stderr.write('%s\n' % str(e))
        env.assertContains('timeout', str(e))
