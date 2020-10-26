GearsBuilder() \
.foreach(lambda x: execute('XADD', "cmd", '*', *sum([[k,v] for k,v in x.items()],[]))) \
.register(prefix="person:*", eventTypes=['hset', 'hmset'])
