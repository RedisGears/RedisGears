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

def testBasicStream(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "GearsBuilder()."
                                  "filter(lambda x:x['key'] != 'values')."
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
                               "map(lambda x: str(x))."
                               "repartition(lambda x: 'new_key')."
                               "foreach(lambda x: redisgears.executeCommand('set', 'new_key', x))."
                               "register('s*')").ok()
    conn.execute_command('xadd', 'stream1', '*', 'name', 'test')
    res = []

    try:
        with TimeLimit(5):
            res = ''
            while res is None or "{'name': 'test', 'streamId': " not in res:
                res = conn.get('new_key')
    except Exception:
        env.assertTrue(False, message='Failed get correct data from new_key')

    conn.execute_command('xadd', 'stream2', '*', 'name', 'test1')

    try:
        with TimeLimit(5):
            res = ''
            while res is None or "{'name': 'test1', 'streamId': " not in res:
                res = conn.get('new_key')
    except Exception:
        env.assertTrue(False, message='Failed get correct data from new_key')

    conn.execute_command('xadd', 'rstream1', '*', 'name', 'test2')
    env.assertContains("{'name': 'test1', 'streamId': ", conn.get('new_key'))

    time.sleep(0.1) # waiting for all the execution to be fully created on all the shards

    # delete all registrations and executions so valgrind check will pass
    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
        env.broadcast('rg.getresultsblocking', r[1])
        env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
        env.expect('RG.UNREGISTER', r[1]).equal('OK')


def testBasicStreamProcessing(env):
    conn = getConnectionByEnv(env)
    res = env.cmd('rg.pyexecute', "GearsBuilder('StreamReader')."
                                  "flatmap(lambda x: [(a[0], a[1]) for a in x.items()])."
                                  "repartition(lambda x: x[0])."
                                  "foreach(lambda x: redisgears.executeCommand('set', x[0], x[1]))."
                                  "map(lambda x: str(x))."
                                  "register('stream1')", 'UNBLOCKING')
    env.assertEqual(res, 'OK')
    if(res != 'OK'):
        return
    time.sleep(0.5)  # make sure the execution reached to all shards
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
    env.cmd('rg.pyexecute', "GB().map(lambda x: ('pref2:' + x['key'].split(':')[1], x['value']))."
                            "repartition(lambda x: x[0])."
                            "foreach(lambda x: execute('set', x[0], x[1]))."
                            "register(regex='pref1:*')")

    time.sleep(0.1) ## wait for execution to get to all shards

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
                            "foreach(lambda x: execute('incrby', 'NumOfKeys{%s}' % (hashtag()), ('1' if 'value' in x.keys() else '-1')))."
                            "register(mode='async_local')")

    time.sleep(0.1) # wait for execution to reach all the shards

    for _ in env.reloading_iterator():
        for i in range(100):
            conn.set(str(i), str(i))

        for i in range(100):
            conn.delete(str(i))

        # wait for all executions to finish
        res = 0
        while res < 200:
            res = env.cmd('rg.pyexecute', "GB('ShardsIDReader').map(lambda x: len([r for r in execute('rg.dumpexecutions') if r[3] == 'done'])).aggregate(0, lambda a, x: x, lambda a, x: a + x).run()")

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

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
         env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

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

    time.sleep(0.1) # wait for reach all shards

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
