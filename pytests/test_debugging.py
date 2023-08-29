from common import gearsTest
from common import toDictionary
from common import runUntil
from redis import Redis
from websockets.client import ClientConnection
import time

MODULE_NAME = "redisgears_2"

@gearsTest()
def testDeployConnectStartDisconnect(env):
    script = '''#!js api_version=1.0 name=foo
redis.registerFunction("test", function(client) {
    return 1
})
    '''

    env.expect('TFUNCTION', 'DELETE', 'foo')
    env.expect('TFUNCTION', 'LOAD', 'debug', script).equal('OK')
    client = ClientConnection("ws://127.0.0.1:9005")
