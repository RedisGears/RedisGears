gb = GearsBuilder('StreamReader')
gb.foreach(lambda x: execute('HMSET', x['streamId'], *x))  # write to Redis Hash
gb.register('mystream')
