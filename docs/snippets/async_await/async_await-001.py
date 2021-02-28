from threading import Lock

consumers = []
publishers = []

class MyMutex():
    def __init__(self):
        self.mutex = Lock()

    def __enter__(self):
        self.mutex.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.mutex.release()

    def __getstate__(self):
        return {}

mutex = None

async def publish(r):
    msg = r[1]
    with mutex:
        if len(consumers) > 0:
            consumer = consumers.pop()
            setFutureResults(consumer, msg)
            return '0K'
        f = createFuture()
        publishers.append((f, msg))
    return await f


async def consume(r):
    with mutex:
        if len(publishers) > 0:
            publisher, msg = publishers.pop()
            setFutureResults(publisher, 'OK')
            return msg
        f = createFuture()
        consumers.append(f)
    return await f

def initializeMutex():
    global mutex
    mutex = MyMutex()

GB('CommandReader').map(publish).register(trigger='MSG_PUBLISH')
GB('CommandReader').map(consume).register(trigger='MSG_CONSUME', onRegistered=initializeMutex)