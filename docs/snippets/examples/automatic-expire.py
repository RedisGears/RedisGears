gb = GB()
gb.foreach(lambda x: execute('EXPIRE', x['key'], 3600))
gb.register('*', mode='sync', readValue=False)
