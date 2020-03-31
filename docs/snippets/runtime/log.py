# Dumps every datum in the DB to the log for "debug" purposes
GB().foreach(lambda x: Log(str(x), level='debug')).run()
