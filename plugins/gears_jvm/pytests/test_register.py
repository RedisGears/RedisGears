from common import jvmTestDecorator
from common import putKeys
import time

@jvmTestDecorator()
def testBasicRegistration(env, results, conn, **kargs):
    env.assertEqual(results, 'OK')
    time.sleep(0.5)  # make sure the registration reached to all shards
    conn.execute_command('set', 'x', '1')
    conn.execute_command('set', 'y', '2')
    conn.execute_command('set', 'z', '3')
    res = []
    while len(res) < 6:
        res = env.cmd('rg.dumpexecutions')
    for e in res:
        env.broadcast('rg.getresultsblocking', e[1])
        env.cmd('rg.dropexecution', e[1])
    env.assertEqual(set(conn.lrange('values', '0', '-1')), set(['1', '2', '3']))
