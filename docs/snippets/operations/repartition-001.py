# Will not repartition anything because the record's key is returned as-is
gb = GB()
gb.repartition(lambda x: x['key'])
gb.run()
