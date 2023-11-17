import signal
from includes import *
from threading import Thread
from RLTest import Env, Defaults
import inspect
import unittest
import json

class Colors(object):
    @staticmethod
    def Cyan(data):
        return '\033[36m' + data + '\033[0m'

    @staticmethod
    def Yellow(data):
        return '\033[33m' + data + '\033[0m'

    @staticmethod
    def Bold(data):
        return '\033[1m' + data + '\033[0m'

    @staticmethod
    def Bred(data):
        return '\033[31;1m' + data + '\033[0m'

    @staticmethod
    def Gray(data):
        return '\033[30;1m' + data + '\033[0m'

    @staticmethod
    def Lgray(data):
        return '\033[30;47m' + data + '\033[0m'

    @staticmethod
    def Blue(data):
        return '\033[34m' + data + '\033[0m'

    @staticmethod
    def Green(data):
        return '\033[32m' + data + '\033[0m'

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

def verifyRegistrationIntegrity(env):
    with TimeLimit(40, env, 'Failed on registrations integrity'):
        while True:
            registrations = set()
            for i in range(1, env.shardsCount + 1, 1):
                c = env.getConnection(i)
                res = c.execute_command('RG.DUMPREGISTRATIONS')
                registrations.add(len(res))
            if len(registrations) == 1:
                break
            time.sleep(0.5)
    env.assertTrue(env.isUp())

def extractInfoOnfailure(env, suffixFileName):
    for i in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId=i)
        conn.set_response_callback('info', lambda r: r)
        shardInfo = {}
        shardInfo['RG.DUMPREGISTRATIONS'] = conn.execute_command('RG.DUMPREGISTRATIONS')
        shardInfo['RG.DUMPEXECUTIONS'] = conn.execute_command('RG.DUMPEXECUTIONS')
        shardInfo['info_everything'] = conn.execute_command('info', 'everything')
        directory = conn.execute_command('config', 'get', 'dir')[1]
        if type(directory) == bytes:
            directory = directory.decode('utf8')
        fileName = conn.execute_command('config', 'get', 'logfile')[1]
        if type(fileName) == bytes:
            fileName = fileName.decode('utf8')
        with open(os.path.join(directory, '%s.failure_logs_%s.txt' % (fileName, suffixFileName)), 'wt') as f:
            f.write(json.dumps(shardInfo, indent=4, sort_keys=True))

def dropRegistrationsAndExecutions(env):
    script1 = '''
GB('ShardsIDReader').map(lambda x: len(execute('RG.DUMPREGISTRATIONS'))).filter(lambda x: x > 0).run()
'''
    script2 = '''
GB('ShardsIDReader').map(lambda x: len(execute('RG.DUMPEXECUTIONS'))).filter(lambda x: x > 0).run()
'''
    try:
        with TimeLimit(40):
            while True:
                try:
                    executions = env.cmd('RG.DUMPEXECUTIONS')
                    for e in executions:
                        env.cmd('RG.DROPEXECUTION', e[1])

                    registrations = env.cmd('RG.DUMPREGISTRATIONS')
                    for r in registrations:
                        env.expect('RG.UNREGISTER', r[1]).equal('OK')
                except Exception as e:
                    print(Colors.Gray(str(e)))
                    time.sleep(0.5)
                    continue
                res1 = env.cmd('RG.PYEXECUTE', script1)
                res2 = env.cmd('RG.PYEXECUTE', script2)
                if len(res1[0]) == 0 and len([a for a in res2[0] if (a != '1' and a != b'1')]) == 0:
                    break
                time.sleep(0.5)
    except Exception as e:
        print(Colors.Bred(str(e)))
        env.assertTrue(False, message='Registrations/Executions dropping failed')

def restoreDefaultConfig(env):
    env.broadcast('RG.CONFIGSET', 'MaxExecutions', '1000')
    env.broadcast('RG.CONFIGSET', 'MaxExecutionsPerRegistration', '100')
    env.broadcast('RG.CONFIGSET', 'MaxExecutionsPerRegistration', '100')
    env.broadcast('RG.CONFIGSET', 'ProfileExecutions', '0')
    env.broadcast('RG.CONFIGSET', 'PythonAttemptTraceback', '1')
    env.broadcast('RG.CONFIGSET', 'ExecutionMaxIdleTime', '5000')
    env.broadcast('RG.CONFIGSET', 'PythonInstallReqMaxIdleTime', '30000')
    env.broadcast('RG.CONFIGSET', 'SendMsgRetries', '3')

def gearsTest(skipTest=False,
              skipOnCluster=False,
              skipCleanups=False,
              skipOnSingleShard=False,
              skipCallback=None,
              skipOnRedis6=False,
              skipWithTLS=False,
              decodeResponses=True,
              executionMaxIdleTime=20000,
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
            env = Env(testName = test_function.__name__, decodeResponses=decodeResponses, **envArgs)
            if env.isCluster():
                # make sure cluster will not turn to failed state and we will not be 
                # able to execute commands on shards, on slow envs, run with valgrind,
                # or mac, it is needed.
                env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '60000')
            env.broadcast('RG.CONFIGSET', 'ExecutionMaxIdleTime', str(executionMaxIdleTime))
            conn = getConnectionByEnv(env)
            version = env.cmd('info', 'server')['redis_version']
            if skipOnRedis6 and '6.0' in version:
                env.skip()
            test_function(env)
            if len(env.assertionFailedSummary) > 0:
                extractInfoOnfailure(env, 'before_cleanups')
            if not skipCleanups:
                dropRegistrationsAndExecutions(env)
                restoreDefaultConfig(env)
            if len(env.assertionFailedSummary) > 0:
                extractInfoOnfailure(env, 'after_cleanups')
        return test_func
    return test_func_generator
