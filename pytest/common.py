import signal

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
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

def verifyRegistrationIntegrity(env):
    scripts = ['''
GB('ShardsIDReader').map(lambda x: len(execute('RG.DUMPREGISTRATIONS'))).collect().distinct().count().run()    
''']
    for script in scripts:
        try:
            with TimeLimit(40):
                while True:
                    res = env.cmd('RG.PYEXECUTE', script)
                    if int(res[0][0]) == 1:
                        break
                    time.sleep(0.5)
        except Exception as e:
            print(str(e))
            env.assertTrue(False, message='Registrations Integrity failed')

        env.assertTrue(env.isUp())
