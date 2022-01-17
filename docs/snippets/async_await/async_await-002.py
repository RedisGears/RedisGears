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
        index = len(publishers) - 1
        async def timeout():
            with mutex:
                if publishers[index][0] == f:
                    future, message = publishers.pop(index)
                    setFutureException(future, 'timedout')
        runCoroutine(timeout(), delay=5)
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

GB('CommandReader').map(publish).register(trigger='MSG_PUBLISH1')
GB('CommandReader').map(consume).register(trigger='MSG_CONSUME1', onRegistered=initializeMutex)