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
        self.t.join()

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
            with TimeLimit(1):
                while bk.isAlive:
                    conn.execute_command('set', 'x', '1')
                    time.sleep(0.1)
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for WaitForKeyChange to reach unblock')
