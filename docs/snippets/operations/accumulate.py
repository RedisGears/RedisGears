# Sum the lengths of all records' keys
gb = GB()
gb.accumulate(lambda a, r: (a if a else 0) + len(r['key']))
gb.run()
