import gevent.server
import gevent.queue
import gevent.socket
import signal
import time
from RLTest import Env


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
        raise Exception()


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


new_conns = gevent.queue.Queue()


def initializeShardMock(env):

    def _handle_conn(sock, client_addr):
        conn = Connection(sock)
        new_conns.put(conn)

    stream_server = gevent.server.StreamServer(('localhost', 10000), _handle_conn)
    stream_server.start()
    env.cmd('RG.CLUSTERSET',
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
    conn = new_conns.get(block=True, timeout=None)
    env.assertEqual(conn.read_request(), ['AUTH', 'password'])
    env.assertEqual(conn.read_request(), ['RG.HELLO'])
    conn.send_status('OK')  # auth response
    conn.send_bulk('1')  # hello response, sending runid
    conn.flush()
    return conn


def testBasicTcpDisconnection(env):
    env.skipOnCluster()

    conn = initializeShardMock(env)

    env.cmd('set', 'x', '1')

    env.cmd('RG.PYEXECUTE', 'GB().repartition(lambda x: x["key"]).run()', 'UNBLOCKING')

    execution = conn.read_request()  # read execution
    conn.send_status('OK')  # got the execution

    eid = execution[3][len(execution[3]) - 48 - 11: len(execution[3]) - 11]  # extracting execution id

    # notify the shard that the execution was recieved
    env.cmd('rg.innermsgcommand', '0000000000000000000000000000000000000002', 'ExecutionPlan_NotifyReceived', eid, '0')

    # reading ExecutionPlan_NotifyRun
    env.assertEqual(conn.read_request()[2], 'ExecutionPlan_NotifyRun')
    # sending Ok ExecutionPlan_NotifyRun
    conn.send_status('OK')

    # reading ExecutionPlan_OnRepartitionRecordReceived
    env.assertEqual(conn.read_request()[2], 'ExecutionPlan_OnRepartitionRecordReceived')

    # not sending OK on ExecutionPlan_OnRepartitionRecordReceived and closing the connection
    conn.close()

    # waiting for the shard to reconect
    conn = new_conns.get(block=True, timeout=None)
    env.assertEqual(conn.read_request(), ['AUTH', 'password'])
    env.assertEqual(conn.read_request(), ['RG.HELLO'])
    conn.send_status('OK')  # auth response
    conn.send_bulk('1')  # hello response, sending same runid so shard will continue from where it stopped
    conn.flush()

    # make sure we got the ExecutionPlan_OnRepartitionRecordReceived again
    env.assertEqual(conn.read_request()[2], 'ExecutionPlan_OnRepartitionRecordReceived')
