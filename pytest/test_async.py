from RLTest import Env
from common import getConnectionByEnv
from common import TimeLimit
from common import verifyRegistrationIntegrity
from threading import Thread
import time

class Background(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def doJob(self):
        self.f()
        self.isAlive = False

    def __init__(self, f):
        self.f = f
        self.isAlive = True

    def __enter__(self):
        self.t = Thread(target = self.doJob)
        self.t.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

def testSimpleAsync(env):
    conn = getConnectionByEnv(env)
    script = '''
class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

blocked = []
def WaitForKeyChange(r):
    f = gearsFuture()
    blocked.append(BlockHolder(f))
    return f
GB('CommandReader').map(WaitForKeyChange).register(trigger='WaitForKeyChange', mode='async_local')

def ForEach(r):
    def unblock(x):
        global blocked
        try:
            [a.continueRun(x['key']) for a in blocked]
            blocked = []
        except Exception as e:
            print(e)
    GB('ShardsIDReader').map(lambda x: r).foreach(unblock).run()
GB().foreach(ForEach).register(mode='sync')
    '''
    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def WaitForKey():
        env.expect('RG.TRIGGER', 'WaitForKeyChange').equal(['x'])

    try:
        with Background(WaitForKey) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('set', 'x', '1')
                    time.sleep(0.1)
    except Exception as e:  
        env.assertTrue(False, message='Failed waiting for WaitForKeyChange to reach unblock')

def testSimpleAsyncOnSyncExecution(env):
    conn = getConnectionByEnv(env)
    script = '''
fdata = []

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    f = BlockHolder(gearsFuture())
    fdata.insert(0, (f, r))
    return f.bc

def unbc_internal(r):
    global fdata
    [a[0].continueRun(a[1]) for a in fdata]
    return r

def unbc(r):
    f = BlockHolder(gearsFuture())
    GB('ShardsIDReader').foreach(unbc_internal).count().foreach(lambda r: f.continueRun(r)).run()
    return f.bc

GB('CommandReader').map(bc).register(trigger='block', mode='sync')
GB('CommandReader').map(unbc).register(trigger='unblock', mode='sync')
    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def Block():
        env.expect('RG.TRIGGER', 'block', 'arg').equal(["['block', 'arg']"])

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock')
                    time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testStreamReaderAsync(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    script = '''
fdata = []

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    f = BlockHolder(gearsFuture())
    fd = (f, r)
    fdata.insert(0, fd)
    return f.bc

GB('CommandReader').map(lambda a: fdata.pop()).foreach(lambda x: x[0].continueRun(x[1])).register(trigger='unblock')
GB('StreamReader').map(bc).foreach(lambda x: execute('set', x['value']['key'], x['value']['val'])).register(mode='sync', prefix='s')

    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    env.cmd('xadd', 's' , '*', 'key', 'x', 'val', '1')
    env.cmd('xadd', 's' , '*', 'key', 'y', 'val', '2')
    env.cmd('xadd', 's' , '*', 'key', 'z', 'val', '3')

    try:
        with TimeLimit(50):
            env.cmd('RG.TRIGGER', 'unblock')
            x = None
            while x != '1':
                x = env.cmd('get', 'x')
                time.sleep(0.1)
            env.cmd('RG.TRIGGER', 'unblock')
            y = None
            while y != '2':
                y = env.cmd('get', 'y')
                time.sleep(0.1)
            env.cmd('RG.TRIGGER', 'unblock')
            z = None
            while z != '3':
                z = env.cmd('get', 'z')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testKeysReaderAsync(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    script = '''
fdata = None

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    if fdata:
        fdata[0].continueRun(fdata[1])   
    f = BlockHolder(gearsFuture())
    fdata = (f, r['key'])
    return f.bc

GB().map(bc).foreach(lambda x: execute('del', x)).register(mode='sync', readValue=False, eventTypes=['set'])

    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def SetX():
        env.expect('set', 'x' , '1')

    def SetY():
        env.expect('set', 'y' , '1')

    try:
        with TimeLimit(50):
            with Background(SetX) as setx:
                x = None
                while x != '1':
                    x = conn.execute_command('GET', 'x')
                    time.sleep(0.1)

                with Background(SetY) as sety:
                    y = None
                    while y != '1':
                        y = conn.execute_command('GET', 'y')
                        time.sleep(0.1)

                    x = conn.execute_command('GET', 'x')
                    while x:
                        x = conn.execute_command('GET', 'x')
                        time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

    executions = env.cmd('RG.DUMPEXECUTIONS')
    for r in executions:
        env.expect('RG.ABORTEXECUTION', r[1]).equal('OK')
        env.expect('RG.DROPEXECUTION', r[1]).equal('OK')

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
         env.expect('RG.UNREGISTER', r[1]).equal('OK')

def testAsyncWithRepartition(env):
    conn = getConnectionByEnv(env)
    script = '''
fdata = []

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    f = BlockHolder(gearsFuture())
    fdata.append((f, r))
    return f.bc

def unbc(r):
    num = 0
    while len(fdata) > 0:
        a = fdata.pop()
        a[0].continueRun(a[1])
        num += 1
    return num

GB('CommandReader').map(bc).count().map(bc).register(trigger='blockcountshards')
GB('CommandReader').map(unbc).register(trigger='unblock')
    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def Block():
        env.expect('RG.TRIGGER', 'blockcountshards', 'arg').equal([str(env.shardsCount)])

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')


def testAsyncWithRepartition2(env):
    conn = getConnectionByEnv(env)
    script = '''
fdata = []

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    f = BlockHolder(gearsFuture())
    fdata.append((f, r))
    return f.bc

def unbc(r):
    num = 0
    while len(fdata) > 0:
        a = fdata.pop()
        a[0].continueRun(a[1])
        num += 1
    return num

GB('CommandReader').flatmap(lambda x: execute('keys', '*')).map(lambda x: execute('get', x)).repartition(lambda x: x).map(bc).count().register(trigger='block')
GB('CommandReader').map(unbc).register(trigger='unblock')
    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    for i in range(10000):
        conn.execute_command('set', i, i)

    def Block():
        env.expect('RG.TRIGGER', 'block').equal(['10000'])

    try:
        with Background(Block) as bk1:
            with Background(Block) as bk2:
                with TimeLimit(50):
                    while bk1.isAlive or bk2.isAlive:
                        conn.execute_command('RG.TRIGGER', 'unblock')
                        time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testSimpleAsyncOnFilter(env):
    conn = getConnectionByEnv(env)
    script = '''
fdata = []

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    f = BlockHolder(gearsFuture())
    fdata.insert(0, f)
    return f.bc

def unbc(r):
    global fdata
    try:
        f = fdata.pop()
    except Exception as e:
        return 0
    if f:
        f.continueRun(True if r[1] == 'true' else False)
        return 1
    return 0

GB('CommandReader').flatmap(lambda x: execute('keys', '*')).collect().filter(bc).count().register(trigger='block')
GB('CommandReader').map(unbc).register(trigger='unblock')
    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    conn.execute_command('set', 'x' , '1')
    conn.execute_command('set', 'y' , '2')
    conn.execute_command('set', 'z' , '3')

    def Block():
        env.expect('RG.TRIGGER', 'block').equal(['3'])

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock', 'true')
                    time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testSimpleAsyncOnFlatMap(env):
    conn = getConnectionByEnv(env)
    script = '''
fdata = None

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    fdata = BlockHolder(gearsFuture())
    return fdata.bc

def unbc(r):
    global fdata
    if fdata:
        fdata.continueRun(r)
        fdata = None
        return 1
    return 0

GB('CommandReader').flatmap(bc).distinct().count().register(trigger='block')
GB('CommandReader').map(unbc).register(trigger='unblock')
    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def Block():
        env.expect('RG.TRIGGER', 'block').equal(['6'])

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock', '1', '2', '3', '4', '5')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testSimpleAsyncOnForeach(env):
    conn = getConnectionByEnv(env)
    script = '''
fdata = None

class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

def bc(r):
    global fdata
    fdata = BlockHolder(gearsFuture())
    return fdata.bc

def unbc(r):
    global fdata
    if fdata:
        fdata.continueRun(r)
        fdata = None
        return 1
    return 0

GB('CommandReader').foreach(bc).flatmap(lambda x: x).distinct().count().register(trigger='block')
GB('CommandReader').map(unbc).register(trigger='unblock')
    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def Block():
        env.expect('RG.TRIGGER', 'block', '1').equal(['2'])

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock', '1', '2', '3', '4', '5')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testSimpleAsyncOnAggregate(env):
    conn = getConnectionByEnv(env)
    script = '''
class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

fdata = None

def unbc(r):
    global fdata
    if fdata:
        fdata[2].continueRun(fdata[0] + fdata[1] * int(r[1]))
        fdata = None
        return 1
    return 0

def doAggregate(a, r):
    global fdata
    fdata = (a if a else 0, r if r else 1, BlockHolder(gearsFuture()))
    return fdata[2].bc

GB('CommandReader').flatmap(lambda x: [int(a) for a in x[1:]]).accumulate(doAggregate).collect().accumulate(doAggregate).register(trigger='block')
GB('CommandReader').map(unbc).register(trigger='unblock')
        '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def Block():
        env.expect('RG.TRIGGER', 'block', '1', '2', '4').equal([str(28 * env.shardsCount)])

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock', '2')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

def testSimpleAsyncOnAggregateBy(env):
    conn = getConnectionByEnv(env)
    script = '''
class BlockHolder:
    def __init__(self, bc):
        self.bc = bc

    def __getstate__(self):
        state = dict(self.__dict__)
        state['bc'] = None
        return state

    def continueRun(self, r):
        if self.bc:
            self.bc.continueRun(r)

fdata = None

def unbc(r):
    global fdata
    if fdata:
        fdata[1].continueRun(fdata[0] + 1)
        fdata = None
        return 1
    return 0

def doGroupBy(k, a, r):
    global fdata
    fdata = (a if a else 0, BlockHolder(gearsFuture()))
    return fdata[1].bc

def toDict(a, r):
    if a == None:
        a = {}
    currVal = a.get(r['key'], 0)
    a[r['key']] = currVal + r['value']
    return a

GB('CommandReader').flatmap(lambda x: [int(a) for a in x[1:]]).groupby(lambda x: x, doGroupBy).collect().accumulate(toDict).register(trigger='block')
GB('CommandReader').map(unbc).register(trigger='unblock')
        '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def Block():
        res = env.cmd('RG.TRIGGER', 'block', '1', '1', '1', '2', '2', '3', '3', '3', '3')
        res = res[0]
        d = eval(res)
        env.assertEqual(d['3'], 4 * env.shardsCount)
        env.assertEqual(d['1'], 3 * env.shardsCount)
        env.assertEqual(d['2'], 2 * env.shardsCount)

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('RG.TRIGGER', 'unblock')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')