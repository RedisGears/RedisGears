import signal
import time
import unittest
import os.path
from RLTest import Env, Defaults
import json

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

def shardsConnections(env):
    for s in range(1, env.shardsCount + 1):
        yield env.getConnection(shardId=s)

def failTest(env, msg):
    env.assertTrue(False, depth=1, message=msg)

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

def getShardInfo(conn, log_file):
    try:
        info = conn.execute_command('info', 'everything')
    except Exception as e:
        info = 'Failed getting shard info, shards probably crashed.'
    try:
        with open(log_file) as f:
            log = f.read()
    except Exception as e:
        log = 'Failed to open log file %s, %s.' % (log_file, str(e))
    
    return {
        'info': info,
        'log': log,
    }

def extractInfoOnfailure(env, log_files):
    shards_data = []
    for index, log_file in enumerate(log_files):
        shards_data.append(getShardInfo(env.getConnection(shardId=index + 1), log_file))
    data = {
        'test_name': env.testName,
        'shards_data': shards_data,
    }
    env.debugPrint(json.dumps(data, indent=2).replace('\\n', '\n'), force=True)

def verifyClusterInitialized(env):
    for conn in shardsConnections(env):
        allConnected = False
        while not allConnected:
            res = conn.execute_command('REDISGEARS_2.INFOCLUSTER')
            nodes = res[4]
            allConnected = True
            for n in nodes:
                status = n[17]
                if status != 'connected':
                    allConnected = False
            if not allConnected:
                time.sleep(0.1)

def gearsTest(skipTest=False,
              skipOnCluster=False,
              skipCleanups=False,
              skipOnSingleShard=False,
              skipCallback=None,
              skipOnRedis6=False,
              skipWithTLS=False,
              decodeResponses=True,
              enableGearsDebugCommands=False,
              cluster=False,
              withReplicas=False,
              shardsCount=2,
              errorVerbosity=1,
              gearsConfig={},
              envArgs={}):
    def test_func_generator(test_function):
        def test_func():
            root_path = os.path.dirname(os.path.dirname(__file__))
            module_path = os.path.join(root_path, 'target/debug/libredisgears.so')
            if not os.path.exists(module_path):
                module_path = os.path.join(root_path, 'target/debug/libredisgears.dylib')
                if not os.path.exists(module_path):
                    raise Exception('Module %s does not exists' % module_path)
            v8_plugin_path = os.path.join(root_path, 'target/debug/libredisgears_v8_plugin.so')
            if not os.path.exists(v8_plugin_path):
                v8_plugin_path = os.path.join(root_path, 'target/debug/libredisgears_v8_plugin.dylib')
                if not os.path.exists(v8_plugin_path):
                    raise Exception('V8 plugin %s does not exists' % v8_plugin_path)
            module_args = [v8_plugin_path]
            if skipTest:
                raise unittest.SkipTest()
            final_envArgs = envArgs.copy()
            if skipOnCluster:
                env = Defaults.env
                if 'env' in final_envArgs.keys():
                    env = final_envArgs['env']
                if 'cluster' in env:
                    raise unittest.SkipTest()
            if 'env' not in final_envArgs.keys():
                if cluster:
                    final_envArgs['env'] = 'oss-cluster'
                    final_envArgs['shardsCount'] = shardsCount
            if withReplicas:
                final_envArgs['useSlaves'] = True
            if skipOnSingleShard and Defaults.num_shards == 1:
                raise unittest.SkipTest()
            if skipWithTLS and Defaults.use_TLS:
                raise unittest.SkipTest()
            if skipCallback is not None:
                if skipCallback():
                    raise unittest.SkipTest()
            if enableGearsDebugCommands:
                module_args += ["enable-debug-command", "yes"]
            for k, v in gearsConfig.items():
                module_args += [k, v]
            module_args += ["error-verbosity", str(errorVerbosity)]
            env = Env(testName = test_function.__name__, decodeResponses=decodeResponses, enableDebugCommand=True, module=module_path, moduleArgs=' '.join(module_args) ,**final_envArgs)
            log_files = []
            for con in shardsConnections(env):
                file_name = con.execute_command('config', 'get', 'logfile')[1]
                file_dir = con.execute_command('config', 'get', 'dir')[1]
                log_file = os.path.abspath(os.path.join(file_dir, file_name))
                log_files.append(log_file)
            if env.isCluster():
                # make sure cluster will not turn to failed state and we will not be
                # able to execute commands on shards, on slow envs, run with valgrind,
                # or mac, it is needed.
                env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '60000')
                env.broadcast('REDISGEARS_2.REFRESHCLUSTER')
                with TimeLimit(2, env, "Failed waiting for cluster to initialized"):
                    verifyClusterInitialized(env)
            if withReplicas:
                # make sure all shards are in sync with their replica
                def synchronise_replicas():
                    replication_info = con.execute_command('info', 'replication')
                    return replication_info['slave0']['state']

                for con in shardsConnections(env):
                    runUntil(env, 'online', synchronise_replicas, timeout=10)
            version = env.cmd('info', 'server')['redis_version']
            if skipOnRedis6 and '6.0' in version:
                env.skip()
            if test_function.__doc__ is not None:
                env.expect('RG.FUNCTION', 'LOAD', test_function.__doc__).equal('OK' if decodeResponses else b'OK')
                if withReplicas:
                    # make sure all shards are in sync with their replica
                    for con in shardsConnections(env):
                        def synchronise_replicas():
                            status = con.execute_command('wait', '1', '0')
                            return status

                        runUntil(env, 1, synchronise_replicas, timeout=10)
            test_args = [env]
            if cluster:
                test_args.append(env.envRunner.getClusterConnection())
            test_function(*test_args)
            if len(env.assertionFailedSummary) > 0:
                extractInfoOnfailure(env, log_files)
        return test_func
    return test_func_generator
