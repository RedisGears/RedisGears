# Will shuffle records by hashing the first byte in their key
gb = GB()
gb.repartition(lambda x: x['key'][:1])
gb.run()
