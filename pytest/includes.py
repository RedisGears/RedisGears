
import sys
import os
import time
from RLTest import Defaults


Defaults.decode_responses = True


try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../deps/readies"))
    import paella
except:
    pass


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
