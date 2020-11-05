GearsBuilder() \
.foreach(lambda x: execute('XADD', "notifications-stream", '*', *sum([[k,v] for k,v in x.items()],[]))) \
.register(prefix="person:*", eventTypes=['hset', 'hmset'], mode='sync')
