from common import gearsTest

@gearsTest()
def testConfigGet(env):
    configs = env.cmd('config', 'get', 'redisgears_2.*')
    config_names = [configs[a].split('.')[1] for a in range(0, len(configs), 2)]
    config_vals = [configs[a] for a in range(1, len(configs), 2)]
    for name in config_names:
        env.cmd('RG.CONFIGGET', name)

    for i in range(0, len(config_names)):
        try:
            env.cmd('RG.CONFIGSET', config_names[i], config_vals[i])
        except Exception:
            pass

@gearsTest()
def testConfigGetNoneExistingConfiguration(env):
    env.expect('RG.CONFIGGET', 'foo').equal([])
    env.expect('RG.CONFIGSET', 'foo', 'bar').error().contains('Unknown option or number of arguments')

@gearsTest()
def testConfigSetErrors(env):
    env.expect('RG.CONFIGSET', 'lock-redis-timeout', 'no_a_number').error().contains('argument couldn\'t be parsed into an integer')
    env.expect('RG.CONFIGSET', 'lock-redis-timeout', '50').error().contains('argument must be between 100 and 1000000000')
    env.expect('RG.CONFIGSET', 'lock-redis-timeout', '1000000001').error().contains('argument must be between 100 and 1000000000')
