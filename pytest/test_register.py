from RLTest import Env
import sys
import os
import time

from common import getConnectionByEnv
from common import TimeLimit

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../deps/readies"))
import paella

class testUnregister:
    def __init__(self):
        self.env = Env()
        self.conn = getConnectionByEnv(self.env)

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
        self.env.assertEqual(self.conn.get('f1'), 'v1')
        self.env.assertEqual(self.conn.get('f2'), 'v2')

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

def testMaxExecutionPerRegistrationStreamReader(env):
    env.skipOnCluster()
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

def testMaxExecutionPerRegistrationKeysReader(env):
    env.skipOnCluster()
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

def testUnregisterKeysReaderWithAbortExecutions():
    env = Env(moduleArgs='executionThreads 1')
    env.skipOnCluster()
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
                registrations = len(env.cmd('RG.DUMPEXECUTIONS'))
                if registrations == 1:
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

def testUnregisterStreamReaderWithAbortExecutions():
    env = Env(moduleArgs='executionThreads 1')
    env.skipOnCluster()
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
                if registrationInfo[0][7][3] == 3 and registrationInfo[0][7][5] == 3:
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
                if registrationInfo[0][7][3] == 7 and registrationInfo[0][7][5] == 5:
                    done = True
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Could not wait for all executions')

    # create another execution, make sure its pending
    eid = env.cmd('rg.pyexecute', 'GB("KeysOnlyReader").run()', 'UNBLOCKING')

    executionsInfo = env.cmd('RG.DUMPEXECUTIONS')
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'done']), 5)
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'running']), 1)
    env.assertEqual(len([a[3] for a in executionsInfo if a[3] == 'created']), 2)

    env.expect('RG.UNREGISTER', registrationId, 'abortpending').ok()

    try:
        with TimeLimit(2):
            while True:
                l = len(env.cmd('RG.DUMPEXECUTIONS'))
                if l == 1:
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
    time.sleep(0.5)  # make sure the execution reached to all shards
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
    res = []
    while len(res) < 1:
        res = env.cmd('rg.dumpexecutions')
    for e in res:
        env.broadcast('rg.getresultsblocking', e[1])
        env.cmd('rg.dropexecution', e[1])
    env.assertEqual(conn.get('f1'), 'v1')
    env.assertEqual(conn.get('f2'), 'v2')

    # delete all registrations and executions so valgrind check will pass
    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

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

def testRegistersReplicatedToSlave():
    env = Env(useSlaves=True, env='oss')
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication
    conn = getConnectionByEnv(env)
    env.cmd('rg.pyexecute', "GB().filter(lambda x: x['key'] != 'NumOfKeys')."
                            "foreach(lambda x: execute('incrby', 'NumOfKeys', ('1' if 'value' in x.keys() else '-1')))."
                            "register()")

    time.sleep(0.5) # wait for registration to reach all the shards

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

def testSyncRegister(env):
    env.skipOnCluster()
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

def testStreamReaderDoNotLoseValues(env):
    env.skipOnCluster()
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

def testStreamReaderWithAof():
    env = Env(env='oss', useAof=True)
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

def testStreamReaderTrimming(env):
    env.skipOnCluster()
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

def testStreamReaderRestartOnSlave():
    script = '''
import time
def FailedOnMaster(r):
    numSlaves = int(execute('info', 'replication').split('\\n')[2].split(':')[1])
    currNum = execute('get', 'NumOfElements')
    if currNum is not None:
        currNum = int(currNum)
    if currNum == 5 and numSlaves == 1:
        execute('set', 'inside_loop', '1')    
        while True:
            time.sleep(1)
    execute('incr', 'NumOfElements')
GB('StreamReader').foreach(FailedOnMaster).register(regex='stream', batch=3)
'''
    env = Env(env='oss', useSlaves=True)
    if env.envRunner.debugger is not None:
        env.skip() # valgrind is not working correctly with replication
    slaveConn = env.getSlaveConnection()
    masterConn = env.getConnection()
    env.cmd('rg.pyexecute', script)
    time.sleep(0.5) # wait for registration to reach all the shards

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

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

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

def testSteamReaderAbortOnFailure(env):
    env.skipOnCluster()

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
                if registrations[0][7][15] == 'ABORTED':
                    break
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for registration to abort')

    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

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

def testCommandReaderBasic(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x).distinct().sort().register(trigger='test1')").ok()
    env.expect('RG.TRIGGER', 'test1', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test1', 'this'])
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x).distinct().sort().register(trigger='test2', mode='sync')").ok()
    env.expect('RG.TRIGGER', 'test2', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test2', 'this'])
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x).distinct().sort().register(trigger='test3', mode='async_local')").ok()
    env.expect('RG.TRIGGER', 'test3', 'this', 'is', 'a', 'test').equal(['a', 'is', 'test', 'test3', 'this'])

def testCommandReaderCluster(env):
    conn = getConnectionByEnv(env)
    env.expect('RG.PYEXECUTE', "GB('CommandReader').count().register(trigger='GetNumShard')").ok()
    env.expect('RG.TRIGGER', 'GetNumShard').equal([str(env.shardsCount)])

def testCommandReaderWithCountBy(env):
    env.skipOnCluster()
    env.expect('RG.PYEXECUTE', "GB('CommandReader').flatmap(lambda x: x[1:]).countby(lambda x: x).register(trigger='test1')").ok()
    env.expect('RG.TRIGGER', 'test1', 'a', 'a', 'a').equal(["{'key': 'a', 'value': 3}"])

    # we need to check twice to make sure the execution reset are not causing issues
    env.expect('RG.TRIGGER', 'test1', 'a', 'a', 'a').equal(["{'key': 'a', 'value': 3}"])

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