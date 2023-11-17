from RLTest import Env
from common import getConnectionByEnv
from common import TimeLimit
from common import verifyRegistrationIntegrity
from common import Background
from common import gearsTest
import time
from includes import *

@gearsTest()
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

async def ForEach(r):
    def unblock(x):
        global blocked
        try:
            [a.continueRun(x['key']) for a in blocked]
            blocked = []
        except Exception as e:
            print(e)
    await GB('ShardsIDReader').map(lambda x: r).foreach(unblock).run()
GB().foreach(ForEach).register(mode='async_local')
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

@gearsTest()
def testSimpleAsyncWithNoneAsyncResult(env):
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

    def continueFailed(self, r):
        if self.bc:
            self.bc.continueFailed(r)

blocked = []
def WaitForKeyChangeReturnSame(r, *args):
    f = gearsFuture()
    blocked.append(BlockHolder(f))
    return r
GB('CommandReader').map(WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeMap', mode='async_local')
GB('CommandReader').flatmap(WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeFlatmap', mode='async_local')
GB('CommandReader').accumulate(WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeAccumulate', mode='async_local')
GB('CommandReader').groupby(lambda x: 'key', WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeAccumulateby', mode='async_local')

def WaitForKeyChangeRaisError(r, *args):
    f = gearsFuture()
    blocked.append(BlockHolder(f))
    raise Exception('test')
GB('CommandReader').foreach(WaitForKeyChangeRaisError).register(trigger='WaitForKeyChangeForeach', mode='async_local')
GB('CommandReader').filter(WaitForKeyChangeRaisError).register(trigger='WaitForKeyChangeFilter', mode='async_local')
GB('CommandReader').map(WaitForKeyChangeRaisError).register(trigger='WaitForKeyChangeMapError', mode='async_local')
GB('CommandReader').flatmap(WaitForKeyChangeRaisError).register(trigger='WaitForKeyChangeFlatmapError', mode='async_local')
GB('CommandReader').accumulate(WaitForKeyChangeRaisError).register(trigger='WaitForKeyChangeAccumulateError', mode='async_local')
GB('CommandReader').groupby(lambda x: 'key', WaitForKeyChangeRaisError).register(trigger='WaitForKeyChangeAccumulatebyError', mode='async_local')

def ForEach(r):
    def unblock(x):
        global blocked
        try:
            [a.continueRun(x['key']) for a in blocked]
            blocked = []
        except Exception as e:
            print(e)
    GB('ShardsIDReader').map(lambda x: r).foreach(unblock).run()
GB().foreach(ForEach).register('x', mode='async_local')

def ForEachFailed(r):
    def unblock(x):
        global blocked
        try:
            [a.continueFailed(x['key']) for a in blocked]
            blocked = []
        except Exception as e:
            print(e)
    GB('ShardsIDReader').map(lambda x: r).foreach(unblock).run()
GB().foreach(ForEachFailed).register('y', mode='async_local')
    '''
    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def WaitForKeyMap():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeMap').equal(["['WaitForKeyChangeMap']"])

    def WaitForKeyChangeForeach():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeForeach').error().contains('Exception: test')

    def WaitForKeyChangeFilter():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeFilter').error().contains('Exception: test')

    def WaitForKeyFlatmap():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeFlatmap').equal(['WaitForKeyChangeFlatmap'])

    def WaitForKeyAccumulate():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeAccumulate').equal(['None'])

    def WaitForKeyAccumulateby():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeAccumulateby').equal(["{'key': 'key', 'value': None}"])

    def WaitForKeyMapError():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeMapError').error().contains('Exception: test')

    def WaitForKeyFlatmapError():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeFlatmapError').error().contains('Exception: test')

    def WaitForKeyAccumulateError():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeAccumulateError').equal(['None'])

    def WaitForKeyAccumulatebyError():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeAccumulatebyError').equal(["{'key': 'key', 'value': None}"])


    tests = [WaitForKeyMap, WaitForKeyChangeForeach, WaitForKeyChangeFilter, 
             WaitForKeyFlatmap, WaitForKeyAccumulate, WaitForKeyAccumulateby,
             WaitForKeyMapError, WaitForKeyFlatmapError, WaitForKeyAccumulateError, WaitForKeyAccumulatebyError]


    for f in tests:
        try:
            with Background(f) as bk:
                with TimeLimit(50):
                    while bk.isAlive:
                        conn.execute_command('set', 'x', '1')
                        time.sleep(1)
        except Exception as e:  
            env.assertTrue(False, message='Failed waiting for WaitForKeyChange to reach unblock')

    for f in tests:
        try:
            with Background(f) as bk:
                with TimeLimit(50):
                    while bk.isAlive:
                        conn.execute_command('set', 'y', '1')
                        time.sleep(1)
        except Exception as e:  
            env.assertTrue(False, message='Failed waiting for WaitForKeyChange to reach unblock')

@gearsTest()
def testCreateAsyncRecordMoreThenOnceRaiseError(env):
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
def WaitForKeyChangeReturnSame(r, *args):
    f1 = gearsFuture()
    blocked.append(BlockHolder(f1))
    f2 = gearsFuture()
    blocked.append(BlockHolder(f2))
    return r
GB('CommandReader').map(WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeMap', mode='async_local')

def ForEach(r):
    def unblock(x):
        global blocked
        try:
            [a.continueRun(x['key']) for a in blocked]
            blocked = []
        except Exception as e:
            print(e)
    GB('ShardsIDReader').map(lambda x: r).foreach(unblock).run()
GB().foreach(ForEach).register(mode='async_local')
    '''
    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def WaitForKeyMap():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeMap').error().contains('Can not create async record twice on the same step')

    try:
        with Background(WaitForKeyMap) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('set', 'x', '1')
                    time.sleep(1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for WaitForKeyChange to reach unblock')

@gearsTest()
def testCreateAsyncWithoutFree(env):
    conn = getConnectionByEnv(env)
    script = '''
def WaitForKeyChangeReturnSame(r, *args):
    f1 = gearsFuture()
    return f1
GB('CommandReader').map(WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeMap', mode='async_local')
    '''
    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    env.expect('RG.TRIGGER', 'WaitForKeyChangeMap').error().contains('Async record did not called continue')

@gearsTest()
def testSetFutureResultsBeforeReturnIt(env):
    conn = getConnectionByEnv(env)
    script = '''
def test(r, *args):
    f1 = gearsFuture()
    f1.continueRun(r)
    return f1
GB('CommandReader').map(test).register(trigger='test', mode='async_local')
    '''
    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    env.expect('RG.TRIGGER', 'test').error().contains('Can not handle future untill it returned from the callback')

@gearsTest()
def testSetFutureErrorOnAggregateByResultsBeforeReturnIt(env):
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

    def continueFailed(self, r):
        if self.bc:
            self.bc.continueFailed(r)

blocked = []
def WaitForKeyChangeReturnSame(r, *args):
    f = gearsFuture()
    blocked.append(BlockHolder(f))
    return f
GB('CommandReader').groupby(lambda x: 'key', WaitForKeyChangeReturnSame).register(trigger='WaitForKeyChangeAccumulateby', mode='async_local')

async def ForEachFailed(r):
    def unblock(x):
        global blocked
        try:
            [a.continueFailed('Failed') for a in blocked]
            blocked = []
        except Exception as e:
            print(e)
    await GB('ShardsIDReader').map(lambda x: r).foreach(unblock).run()
GB().foreach(ForEachFailed).register('y', mode='async_local')
    '''
    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    def WaitForKeyAccumulateby():
        env.expect('RG.TRIGGER', 'WaitForKeyChangeAccumulateby').equal('Failed')

    try:
        with Background(WaitForKeyAccumulateby) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    conn.execute_command('set', 'y', '1')
                    time.sleep(0.1)
    except Exception as e:  
        env.assertTrue(False, message='Failed waiting for WaitForKeyChange to reach unblock')

@gearsTest()
def testSimpleAsyncOnLocalExecutions(env):
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
    try:
        while len(fdata) > 0:
            a = fdata.pop()
            a[0].continueRun(a[1])
    except Exception as e:
        print(e)
    return r

def unbc(r):
    f = BlockHolder(gearsFuture())
    def c(re):
        f.continueRun(r)
    GB('ShardsIDReader').foreach(unbc_internal).count().foreach(c).run()
    return f.bc

GB('CommandReader').map(bc).register(trigger='block', mode='async_local')
GB('CommandReader').map(unbc).register(trigger='unblock', mode='async_local')

GB('CommandReader').map(bc).register(trigger='block_sync', mode='sync')
GB('CommandReader').map(unbc).register(trigger='unblock_sync', mode='sync')
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
                    env.execute_command('RG.TRIGGER', 'unblock')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

    def Block2():
        env.expect('RG.TRIGGER', 'block_sync', 'arg').equal(["['block_sync', 'arg']"])

    try:
        with Background(Block2) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    env.execute_command('RG.TRIGGER', 'unblock_sync')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest(skipOnCluster=True)
def testStreamReaderAsync(env):
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
GB('StreamReader').map(bc).foreach(lambda x: execute('set', x['value']['key'], x['value']['val'])).register(mode='async_local', prefix='s')

    '''

    env.expect('RG.PYEXECUTE', script).ok()

    # this will make sure registrations reached all the shards
    verifyRegistrationIntegrity(env)

    env.cmd('xadd', 's' , '*', 'key', 'x', 'val', '1')
    env.cmd('xadd', 's' , '*', 'key', 'y', 'val', '2')
    env.cmd('xadd', 's' , '*', 'key', 'z', 'val', '3')

    def Unblock():
        while True:
            try:
                env.cmd('RG.TRIGGER', 'unblock')
                break
            except Exception as e:
                pass

    try:
        with TimeLimit(50):
            Unblock()
            x = None
            while x != '1':
                x = env.cmd('get', 'x')
                time.sleep(0.1)
            Unblock()
            y = None
            while y != '2':
                y = env.cmd('get', 'y')
                time.sleep(0.1)
            Unblock()
            z = None
            while z != '3':
                z = env.cmd('get', 'z')
                time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest(skipOnCluster=True)
def testKeysReaderAsync(env):
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

GB().map(bc).foreach(lambda x: execute('del', x)).register(mode='async_local', readValue=False, eventTypes=['set'])

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

@gearsTest()
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
                    env.execute_command('RG.TRIGGER', 'unblock')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
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
                        env.execute_command('RG.TRIGGER', 'unblock')
                        time.sleep(1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
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
                    env.execute_command('RG.TRIGGER', 'unblock', 'true')
                    time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
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
                    env.execute_command('RG.TRIGGER', 'unblock', '1', '2', '3', '4', '5')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
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
                    env.execute_command('RG.TRIGGER', 'unblock', '1', '2', '3', '4', '5')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
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
                    env.execute_command('RG.TRIGGER', 'unblock', '2')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
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
        d = eval(res[0])
        env.assertEqual(d['3'], 4 * env.shardsCount)
        env.assertEqual(d['1'], 3 * env.shardsCount)
        env.assertEqual(d['2'], 2 * env.shardsCount)

    try:
        with Background(Block) as bk:
            with TimeLimit(50):
                while bk.isAlive:
                    env.execute_command('RG.TRIGGER', 'unblock')
                    time.sleep(0.1)
    except Exception as e:
        print(e)
        env.assertTrue(False, message='Failed waiting to reach unblock')

@gearsTest()
def testAsyncError(env):
    conn = getConnectionByEnv(env)

    env.expect('RG.PYEXECUTE', 'gearsFuture()').error().contains('Future object can only be created inside certain execution steps')
    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').repartition(lambda x: gearsFuture()).run()")[1][0]
    env.assertContains('Step does not support async', res)

    res = env.cmd('RG.PYEXECUTE', "GB('ShardsIDReader').batchgroupby(lambda x: x, lambda k, l: gearsFuture()).run()")[1][0]
    env.assertContains('Step does not support async', res)

@gearsTest()
def testAsyncAwait(env):
    conn = getConnectionByEnv(env)
    script = '''
import asyncio

async def c(r):
    await asyncio.sleep(0.1)
    return r

GB('ShardsIDReader').map(c).flatmap(c).foreach(c).filter(c).count().run()
        '''

    env.expect('RG.PYEXECUTE', script).equal([[str(env.shardsCount)], []])

@gearsTest()
def testAsyncAwaitWithSyncExecution(env):
    conn = getConnectionByEnv(env)
    script = '''
import asyncio

async def c(r):
    await asyncio.sleep(0.1)
    return r

GB('CommandReader').map(c).flatmap(c).foreach(c).filter(c).count().register(trigger='test', mode='sync')
        '''

    env.expect('RG.PYEXECUTE', script).equal('OK')
    verifyRegistrationIntegrity(env)    

    env.expect('RG.TRIGGER', 'test').equal(['1'])    

@gearsTest()
def testAsyncAwaitWithSyncExecutionInMultiExec(env):
    conn = getConnectionByEnv(env)
    script = '''

def c(r):
    if isAsyncAllow():
        async def f():
            return 'no multi'
        return f()
    return 'multi'

GB('CommandReader').map(c).register(trigger='test', mode='sync')
        '''

    env.expect('RG.PYEXECUTE', script).equal('OK')
    verifyRegistrationIntegrity(env)    

    env.expect('RG.TRIGGER', 'test').equal(['no multi'])

    env.cmd('multi')
    env.cmd('RG.TRIGGER', 'test')
    env.expect('exec').equal([['multi']])    

@gearsTest()
def testAsyncAwaitOnUnallowRepartitionStep(env):
    conn = getConnectionByEnv(env)
    script = '''
import asyncio

async def c(r):
    await asyncio.sleep(0.1)
    return r

GB('ShardsIDReader').map(c).flatmap(c).foreach(c).filter(c).repartition(c).run()
        '''

    res = env.cmd('RG.PYEXECUTE', script)[1][0]
    env.assertContains('coroutine are not allow on', res)

@gearsTest()
def testAsyncAwaitOnUnallowReduceStep(env):
    conn = getConnectionByEnv(env)
    script = '''
import asyncio

async def c(r):
    await asyncio.sleep(0.1)
    return r

async def reduce(k, l):
    await asyncio.sleep(0.1)
    return l

GB('ShardsIDReader').map(c).flatmap(c).foreach(c).filter(c).batchgroupby(lambda x: x, reduce).run()
        '''

    res = env.cmd('RG.PYEXECUTE', script)[1][0]
    env.assertContains('coroutine are not allow on', res)

@gearsTest()
def testAsyncAwaitThatRaiseException(env):
    conn = getConnectionByEnv(env)
    script = '''
import asyncio

async def c(r):
    await asyncio.sleep(0.1)
    raise Exception('failed')
    return r

GB('ShardsIDReader').map(c).run()
        '''

    res = env.cmd('RG.PYEXECUTE', script)[1][0]
    env.assertContains('failed', res)

@gearsTest(skipOnCluster=True)
def testUnregisterDuringAsyncExectuion(env):
    script = '''
import asyncio

async def doTest(x):
    # delete all registrations so valgrind check will pass
    registrations = execute('RG.DUMPREGISTRATIONS')
    for r in registrations:
        execute('RG.UNREGISTER', r[1], 'abortpending')
    return x

GB("CommandReader").map(doTest).register(trigger='test')
    '''

    env.cmd('rg.pyexecute', script)
    verifyRegistrationIntegrity(env)
    env.expect('rg.trigger', 'test').equal(["['test']"])

    env.expect('RG.DUMPREGISTRATIONS').equal([])

@gearsTest()
def testAbortDuringAsyncExectuion(env):
    script = '''
import asyncio

async def doTest(x):
    await asyncio.sleep(1)
    print('after wait')  
    return x

GB("ShardsIDReader").map(doTest).run()
    '''

    env.cmd('rg.pyexecute', script, 'UNBLOCKING')

    wait_for_execution = True
    with TimeLimit(5, env, 'Failed waiting for execution to start running'):
        while wait_for_execution:
            executions = env.cmd('RG.DUMPEXECUTIONS')
            for e in executions:
                if e[3] == 'running':
                    wait_for_execution = False
                    env.expect('RG.ABORTEXECUTION', e[1]).equal('OK')
                    break
                elif e[3] == 'done':
                    env.debugPrint('execution finished before aborting, test is not testing what it was supposed to test.')
                    wait_for_execution = False
                    env.expect('RG.ABORTEXECUTION', e[1]).equal('OK')
                    break
            time.sleep(0.1)

    # let wait for the coro to continue, make sure there is no issues.
    time.sleep(2)

@gearsTest()
def testAsyncExecutionOnMulti(env):
    conn = getConnectionByEnv(env)
    script = '''
import asyncio

async def c(r):
    await asyncio.sleep(0.1)
    return r

GB('CommandReader').map(c).register(trigger='test')
        '''

    env.expect('RG.PYEXECUTE', script).equal('OK')
    env.expect('MULTI').equal('OK')
    env.expect('RG.TRIGGER', 'test').equal('QUEUED')
    res = env.cmd('EXEC')
    env.assertIn('can not run a non-sync execution inside a MULTI/LUA', str(res[0]))
    
@gearsTest()
def testAsyncWithoutWait(env):
    conn = getConnectionByEnv(env)
    script = '''
import time

def reader():
    yield 1
    time.sleep(1)
    yield 2

async def c(r):
    return r

GB('PythonReader').map(c).count().run(reader)
        '''

    env.expect('RG.PYEXECUTE', script).equal([[str(2 * env.shardsCount)], []])
