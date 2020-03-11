gb = GearsBuilder()
gb.map(lambda x: x['key'])               # map the records to key names
gb.foreach(lambda x: execute('DEL', x))  # delete each key
gb.count()                               # count the records
gb.run('delete_me:*')
