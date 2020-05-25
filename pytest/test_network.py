import gevent.server
import gevent.queue
import gevent.socket
import signal
import time
from RLTest import Env
from common import TimeLimit


class Connection(object):
    def __init__(self, sock, bufsize=4096, underlying_sock=None):
        self.sock = sock
        self.sockf = sock.makefile('rwb', bufsize)
        self.closed = False
        self.peer_closed = False
        self.underlying_sock = underlying_sock

    def close(self):
        if not self.closed:
            self.closed = True
            self.sockf.close()
            self.sock.close()
            self.sockf = None

    def is_close(self, timeout=2):
        if self.closed:
            return True
        try:
            with TimeLimit(timeout):
                return self.read(1) == ''
        except Exception:
            return False

    def flush(self):
        self.sockf.flush()

    def get_address(self):
        return self.sock.getsockname()[0]

    def get_port(self):
        return self.sock.getsockname()[1]

    def read(self, bytes):
        return self.sockf.read(bytes)

    def read_at_most(self, bytes, timeout=0.01):
        self.sock.settimeout(timeout)
        return self.sock.recv(bytes)

    def send(self, data):
        self.sockf.write(data)
        self.sockf.flush()

    def readline(self):
        return self.sockf.readline()

    def send_bulk_header(self, data_len):
        self.sockf.write('$%d\r\n' % data_len)
        self.sockf.flush()

    def send_bulk(self, data):
        self.sockf.write('$%d\r\n%s\r\n' % (len(data), data))
        self.sockf.flush()

    def send_status(self, data):
        self.sockf.write('+%s\r\n' % data)
        self.sockf.flush()

    def send_integer(self, data):
        self.sockf.write(':%u\r\n' % data)
        self.sockf.flush()

    def send_mbulk(self, data):
        self.sockf.write('*%d\r\n' % len(data))
        for elem in data:
            self.sockf.write('$%d\r\n%s\r\n' % (len(elem), elem))
        self.sockf.flush()

    def read_mbulk(self, args_count=None):
        if args_count is None:
            line = self.readline()
            if not line:
                self.peer_closed = True
            if not line or line[0] != '*':
                self.close()
                return None
            try:
                args_count = int(line[1:])
            except ValueError:
                raise Exception('Invalid mbulk header: %s' % line)
        data = []
        for arg in range(args_count):
            data.append(self.read_response())
        return data

    def read_request(self):
        line = self.readline()
        if not line:
            self.peer_closed = True
            self.close()
            return None
        if line[0] != '*':
            return line.rstrip().split()
        try:
            args_count = int(line[1:])
        except ValueError:
            raise Exception('Invalid mbulk request: %s' % line)
        return self.read_mbulk(args_count)

    def read_request_and_reply_status(self, status):
        req = self.read_request()
        if not req:
            return
        self.current_request = req
        self.send_status(status)

    def wait_until_writable(self, timeout=None):
        try:
            gevent.socket.wait_write(self.sockf.fileno(), timeout)
        except gevent.socket.error:
            return False
        return True

    def wait_until_readable(self, timeout=None):
        if self.closed:
            return False
        try:
            gevent.socket.wait_read(self.sockf.fileno(), timeout)
        except gevent.socket.error:
            return False
        return True

    def read_response(self):
        line = self.readline()
        if not line:
            self.peer_closed = True
            self.close()
            return None
        if line[0] == '+':
            return line.rstrip()
        elif line[0] == ':':
            try:
                return int(line[1:])
            except ValueError:
                raise Exception('Invalid numeric value: %s' % line)
        elif line[0] == '-':
            return line.rstrip()
        elif line[0] == '$':
            try:
                bulk_len = int(line[1:])
            except ValueError:
                raise Exception('Invalid bulk response: %s' % line)
            if bulk_len == -1:
                return None
            data = self.sockf.read(bulk_len + 2)
            if len(data) < bulk_len:
                self.peer_closed = True
                self.close()
            return data[:bulk_len]
        elif line[0] == '*':
            try:
                args_count = int(line[1:])
            except ValueError:
                raise Exception('Invalid mbulk response: %s' % line)
            return self.read_mbulk(args_count)
        else:
            raise Exception('Invalid response: %s' % line)


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
                     '2',
                     'NO-USED',
                     '1',
                     'NO-USED',
                     '0',
                     '8192',
                     'NO-USED',
                     'password@localhost:6379',
                     'NO-USED',
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

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])
        conn.send_status('OK')

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])
        conn.send_status('OK')


def testMessageResentAfterDisconnect(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        conn = shardMock.GetConnection()

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send_status('OK')

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])

        conn.close()

        conn = shardMock.GetConnection()

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])

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

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send_status('OK')

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])

        conn.close()

        conn = shardMock.GetConnection(runid='2')  # shard crash

        try:
            with TimeLimit(1):
                conn.read_request()
                env.assertTrue(False)  # we should not get any data after crash
        except Exception:
            pass


def testSendRetriesMechanizm(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        conn = shardMock.GetConnection()

        env.expect('RG.NETWORKTEST').equal('OK')

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send('-Err\r\n')

        env.assertTrue(conn.is_close())

        # should be a retry

        conn = shardMock.GetConnection()

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send('-Err\r\n')

        env.assertTrue(conn.is_close())

        # should be a retry

        conn = shardMock.GetConnection()

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send('-Err\r\n')

        env.assertTrue(conn.is_close())

        # should not retry

        conn = shardMock.GetConnection()

        # make sure message will not be sent again
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

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '0'])

        conn.send_status('OK')

        shardMock.StopListening()

        conn.close()

        time.sleep(0.5)

        env.expect('RG.NETWORKTEST').equal('OK')

        shardMock.StartListening()

        conn = shardMock.GetConnection()

        env.assertEqual(conn.read_request(), ['RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000001', 'RG_NetworkTest', 'test', '1'])



def testDuplicateMessagesAreIgnored(env):
    env.skipOnCluster()

    with ShardMock(env) as shardMock:
        shardMock.GetConnection()
        env.expect('RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000002', 'RG_NetworkTest', 'test', '0').equal('OK')
        env.expect('RG.INNERMSGCOMMAND', '0000000000000000000000000000000000000002', 'RG_NetworkTest', 'test', '0').equal('duplicate message ignored')
