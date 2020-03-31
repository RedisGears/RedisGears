# Will put all values in a single Python list
gb = GB()
gb.aggregate([],
             lambda a, r: a + [r['value']],
             lambda a, r: a + r)
gb.run()
