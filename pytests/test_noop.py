from common import gearsTest
import os

MODULE_NAME = "redisgears_2"

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "../.."))
TESTS_ROOT = os.path.abspath(os.path.join(HERE, ".."))
JSON_PATH = os.path.join(TESTS_ROOT, 'files')

"""
Should start redis and load the data from a custom database dump file,
but not load any gears scripts (functions or triggers).
"""
@gearsTest()
def testReadsRdbButLoadsNothing(env):
    env.skipOnCluster()

    if env.useAof:
        env.skip()

    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()

    try:
        os.unlink(rdbFilePath)
    except OSError:
        pass

    filePath = os.path.join(JSON_PATH, 'triggers_and_functions.rdb')
    os.symlink(filePath, rdbFilePath)
    env.start()

    env.expect('TFUNCTION', 'LIST', 'vv').equal(None)

    # Functions
    env.expectTfcall('gears_example', 'foo').equal(None)
    env.assertEqual(env.cmd('get', 'triggers_counter'), None)
    env.assertEqual(env.tfcallAsync('gears_example', 'asyncfoo'), None)
    env.assertEqual(env.cmd('get', 'triggers_counter'), None)

    # Key triggers
    env.cmd('set', 'key1', 'bar')
    env.assertEqual(env.cmd('get', 'triggers_counter'), None)

    # Stream triggers
    env.cmd('xadd', 'stream1', '*', 'foo', 'bar')
    env.assertEqual(env.cmd('get', 'triggers_counter'), None)
