from RLTest import Env
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

BASE_JAR_FILE = '../gears_tests/gears_tests.jar'

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def jvmTestDecorator(preExecute=None):
    def jvmTest(testFunc):
        def jvmTestFunc():
            testName = 'gears_tests.%s' % testFunc.func_name 
            print(Colors.Cyan('\tRunning: %s' % testName))
            env = Env()
            conn = getConnectionByEnv(env)
            if preExecute is not None:
                preExecute(conn)
            with open(BASE_JAR_FILE, 'rb') as f:
                data = f.read()
                res = env.cmd('RG.JEXECUTE', testName, data)
            results = [json.loads(r) for r in res[0]]
            errs = res[1]
            for err in errs:
                print(Colors.Bred('\tError reply: %s' % err))
            testFunc(env, results, errs)
        return jvmTestFunc
    return jvmTest