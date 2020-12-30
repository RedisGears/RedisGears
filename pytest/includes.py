
import sys
import os

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../deps/readies"))
    import paella
except:
    pass


def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn
