from common import gearsTest

@gearsTest()
def testConfigGet(env):
    configs = env.cmd('config', 'get', 'redisgears_2.*')
    config_names = [configs[a].split('.')[1] for a in range(0, len(configs), 2)]
    config_vals = [configs[a] for a in range(1, len(configs), 2)]
    for name in config_names:
        env.cmd('RG.CONFIG', 'GET', name)

    for i in range(0, len(config_names)):
        try:
            env.cmd('RG.CONFIG', 'SET', config_names[i], config_vals[i])
        except Exception:
            pass

@gearsTest()
def testConfigGetNoneExistingConfiguration(env):
    env.expect('RG.CONFIG', 'GET', 'foo').error().contains('No such configuration')
    env.expect('RG.CONFIG', 'SET', 'foo', 'bar').error().contains('No such configuration')

@gearsTest()
def testConfigSetErrors(env):
    env.expect('RG.CONFIG', 'SET', 'lock-redis-timeout', 'no_a_number').error().contains('Failed parsing value')
    env.expect('RG.CONFIG', 'SET', 'lock-redis-timeout', '50').error().contains('is less then minimum value allowed')
    env.expect('RG.CONFIG', 'SET', 'lock-redis-timeout', '1000000001').error().contains('is greater then maximum value allowed')

@gearsTest()
def testUnknownConfigSubCommand(env):
    env.expect('RG.CONFIG', 'foo').error().contains('Unknown subcommand')