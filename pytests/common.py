import signal
import time
import unittest
from RLTest import Env, Defaults

def toDictionary(res, max_recursion=1000):
    if  max_recursion == 0:
        return res
    if type(res) != list:
        return res
    if len(res) == 0:
        return res
    if type(res[0]) == list:
        # nested list, keep it as list
        return [toDictionary(r, max_recursion - 1) for r in res]
    return {res[i]: toDictionary(res[i + 1], max_recursion - 1) for i in range(0, len(res), 2)}

def runUntil(env, expected_result, callback, sleep_time=0.1, timeout=1):
    with TimeLimit(timeout, env, "Failed waiting for callback to return '%s'" % str(expected_result)):
        while True:
            try:
                if callback() == expected_result:
                    break
            except Exception:
                pass
            time.sleep(sleep_time)

def runFor(expected_result, callback, sleep_time=0.1, timeout=1):
    try:
        with TimeLimit(timeout):
            while True:
                res = callback()
                if res == expected_result:
                    time.sleep(sleep_time)
                    continue
                raise Exception("Failed, Expected '%s' got '%s'" % (str(expected_result), str(res)))
    except Exception as e:
        if str(e) != 'timeout':
            raise e
            

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout, env=None, msg=None):
        self.timeout = timeout
        self.env = env
        self.msg = msg

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        if self.env is not None:
            self.env.assertTrue(False, message='Timedout %s' % (str(self.msg) if self.msg is not None else 'Error'))
        raise Exception('timeout')

def getConnectionByEnv(env):
    conn = None
    # env.broadcast('rg.refreshcluster')
    if env.env == 'oss-cluster' and env.shardsCount > 1:
        conn = env.envRunner.getClusterConnection()
        # for s in range(1, env.shardsCount + 1):
        #     while True:
        #         c = env.getConnection(shardId=s)
        #         res = c.execute_command('RG.INFOCLUSTER')
        #         if res == 'no cluster mode':
        #             continue
        #         res = res[4]
        #         isAllRunIdsFound = True
        #         for r in res:
        #             if r[9] == None: # runid
        #                 isAllRunIdsFound = False
        #         if isAllRunIdsFound:
        #             break
    else:
        conn = env.getConnection()
    return conn


def extractInfoOnfailure(env, prefix):
    pass

def doCleanups(env):
    pass

def gearsTest(skipTest=False,
              skipOnCluster=False,
              skipCleanups=False,
              skipOnSingleShard=False,
              skipCallback=None,
              skipOnRedis6=False,
              skipWithTLS=False,
              decodeResponses=True,
              envArgs={}):
    def test_func_generator(test_function):
        def test_func():
            if skipTest:
                raise unittest.SkipTest()
            if skipOnCluster:
                env = Defaults.env
                if 'env' in envArgs.keys():
                    env = envArgs['env']
                if 'cluster' in env:
                    raise unittest.SkipTest()
            if skipOnSingleShard and Defaults.num_shards == 1:
                raise unittest.SkipTest()
            if skipWithTLS and Defaults.use_TLS:
                raise unittest.SkipTest()
            if skipCallback is not None:
                if skipCallback():
                    raise unittest.SkipTest()
            env = Env(testName = test_function.__name__, decodeResponses=decodeResponses, enableDebugCommand=True, **envArgs)
            if env.isCluster():
                # make sure cluster will not turn to failed state and we will not be 
                # able to execute commands on shards, on slow envs, run with valgrind,
                # or mac, it is needed.
                env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '60000')
            conn = getConnectionByEnv(env)
            version = env.cmd('info', 'server')['redis_version']
            if skipOnRedis6 and '6.0' in version:
                env.skip()
            if test_function.__doc__ is not None:
                env.expect('RG.FUNCTION', 'LOAD', test_function.__doc__).equal('OK')
            test_function(env)
            if len(env.assertionFailedSummary) > 0:
                extractInfoOnfailure(env, 'before_cleanups')
            if not skipCleanups:
                doCleanups(env)
            if len(env.assertionFailedSummary) > 0:
                extractInfoOnfailure(env, 'after_cleanups')
        return test_func
    return test_func_generator
