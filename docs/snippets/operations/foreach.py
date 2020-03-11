# Increment a shard-local counter for each record processed
gb = GB()
gb.foreach(lambda x: execute('INCR', hashtag()))
gb.run()
