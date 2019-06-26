import gevent.server
import gevent.queue
import gevent.socket
import signal
import time
from RLTest import Env
from connection import Connection, TimeLimit


class ShardMock():
    def __init__(self, env):
        self.env = env
        self.new_conns = gevent.queue.Queue()

    def _handle_conn(self, sock, client_addr):
        conn = Connection(sock)
        self.new_conns.put(conn)

    def __enter__(self):
        self.stream_server = gevent.server.StreamServer(('localhost', 10000), self._handle_conn)
        self.stream_server.start()
        self.env.cmd('RG.CLUSTERSET',
                     'NO-USED',
                     'NO-USED',
                     'NO-USED',
                     'NO-USED',
                     'NO-USED',
                     '1',
                     'NO-USED',
                     '2', # number of shards
                     'NO-USED',
                     '1', # shard id
                     'NO-USED',
                     '0', # first hslot
                     '8192', # last hslot
                     'NO-USED',
                     'password@localhost:6379', # addr + password
                     'NO-USED', # hash policy (optional unix socket should be added here!!!)
                     'NO-USED',
                     '2',
                     'NO-USED',
                     '8193',
                     '16383',
                     'NO-USED',
                     'password@localhost:10000'
                     )
        return self

    def __exit__(self, type, value, traceback):
        self.stream_server.stop()

    def GetConnection(self, runid='1'):
        conn = self.new_conns.get(block=True, timeout=None)
        self.env.assertEqual(conn.read_request(), ['AUTH', 'password'])
        self.env.assertEqual(conn.read_request(), ['RG.HELLO'])
        conn.send_status('OK')  # auth response
        conn.send_bulk(runid)  # hello response, sending runid
        conn.flush()
        return conn

    def StopListening(self):
        self.stream_server.stop()

    def StartListening(self):
        self.stream_server = gevent.server.StreamServer(('localhost', 10000), self._handle_conn)
        self.stream_server.start()


def testMessageIdCorrectness(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        conn = shardMock.GetConnection()

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])
        conn.send_status('OK')

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])
        conn.send_status('OK')


def testMessageResentAfterDisconnect(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        conn = shardMock.GetConnection()

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send_status('OK')

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])

        conn.close()

        conn = shardMock.GetConnection()

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])

        conn.send_status('duplicate message ignored')  # reply to the second message with duplicate reply

        conn.close()

        conn = shardMock.GetConnection()

        # make sure message 2 will not be sent again
        try:
            with TimeLimit(1):
                conn.read_request()
                env.assertTrue(False)  # we should not get any data after crash
        except Exception:
            pass


def testMessageNotResentAfterCrash(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        conn = shardMock.GetConnection()

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send_status('OK')

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])

        conn.close()

        conn = shardMock.GetConnection(runid='2')  # shard crash

        try:
            with TimeLimit(1):
                conn.read_request()
                env.assertTrue(False)  # we should not get any data after crash
        except Exception:
            pass


def testStopListening(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        conn = shardMock.GetConnection()

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send_status('OK')

        shardMock.StopListening()

        conn.close()

        time.sleep(0.5)

        env.expect('RG.NETWORKTEST').equal('OK')

        shardMock.StartListening()

        conn = shardMock.GetConnection()

        env.assertEqual(conn.read_request(), ['rg.innermsgcommand', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])



def testDuplicateMessagesAreIgnored(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        shardMock.GetConnection()
        env.expect('rg.innermsgcommand', '0000000000000000000000000000000000000002', 'RG_NetworkTest', 'test', '0').equal('OK')
        env.expect('rg.innermsgcommand', '0000000000000000000000000000000000000002', 'RG_NetworkTest', 'test', '0').equal('duplicate message ignored')
