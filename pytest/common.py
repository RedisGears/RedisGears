import signal
import time

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster' and env.shardsCount > 1:
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
        for s in range(1, env.shardsCount + 1):
            while True:
                c = env.getConnection(shardId=s)
                res = c.execute_command('RG.INFOCLUSTER')
                if res == 'no cluster mode':
                    continue
                res = res[4]
                isAllRunIdsFound = True
                for r in res:
                    if r[9] == None: # runid
                        isAllRunIdsFound = False
                if isAllRunIdsFound:
                    break
    else:
        conn = env.getConnection()
    return conn

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout):
        self.timeout = timeout

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        raise Exception('timeout')