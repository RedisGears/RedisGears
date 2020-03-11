# Group and count records by the first byte in their key
gb = GB()
gb.batchgroupby(lambda x: x['key'][:1],
                lambda k, l: len(l))
gb.run()
