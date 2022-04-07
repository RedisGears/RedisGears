import os
import subprocess
from includes import *

from common import TimeLimit, gearsTest

GEARS_CACHE_DIR = '/tmp/'
BASE_RDBS_URL = 'https://s3.amazonaws.com/redismodules/redisgears/versions_rdbs_samples/'

class RdbMetaData:
    def __init__(self, fileName, nRegistrations, nExecutions):
        self.fileName = fileName
        self.nRegistrations = nRegistrations
        self.nExecutions = nExecutions
    
    def getRdbMetaData(self):
        return (self.fileName, self.nRegistrations, self.nExecutions)

RDBS = [
    RdbMetaData(fileName='redisgears_1.0.0.rdb', nRegistrations=1, nExecutions=0),
    RdbMetaData(fileName='redisgears_1.0.0_2.rdb', nRegistrations=3, nExecutions=1)
]

def downloadFiles():
    if not os.path.exists(GEARS_CACHE_DIR):
        os.makedirs(GEARS_CACHE_DIR)
    for f, _, _ in [r.getRdbMetaData() for r in RDBS]:
        path = os.path.join(GEARS_CACHE_DIR, f)
        if not os.path.exists(path):
            subprocess.call(['wget', '-q', BASE_RDBS_URL + f, '-O', path])
        if not os.path.exists(path):
            return False
    return True

@gearsTest()
def testRDBCompatibility(env):
    env.skipOnCluster()
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    if not downloadFiles():
        if os.environ.get('CI'):
            env.assertTrue(False)  ## we could not download rdbs and we are running on CI, let fail the test
        else:
            env.skip()
            return

    for fileName, nRegistrations, nExecutions in [r.getRdbMetaData() for r in RDBS]:
        env.stop()
        filePath = os.path.join(GEARS_CACHE_DIR, fileName)
        try:
            os.unlink(rdbFilePath)
        except OSError:
            pass
        os.symlink(filePath, rdbFilePath)
        env.start()
        res = env.cmd('rg.dumpregistrations')
        env.assertEqual(len(res), nRegistrations)
        res = env.cmd('rg.pydumpreqs')
        env.assertEqual(len(res), 1)
        env.assertTrue(env.checkExitCode())

        # wait until we will have enough executions
        try:
            with TimeLimit(5):
                executions = 0
                while executions != nExecutions:
                    r = env.cmd('rg.dumpexecutions')
                    res = [r for r in env.cmd('rg.dumpexecutions') if r[3] == 'done']
                    executions = len(res)
        except Exception as e:
            print(e)
            env.assertTrue(False, message='Could not wait for all executions to finished')


if __name__ == "__main__":
    if not downloadFiles():
        raise Exception("Couldn't download RDB files")
    print("RDB Files ready for testing!")