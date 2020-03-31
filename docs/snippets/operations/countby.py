# Counts the number of time each value is stored
gb = GB()
gb.countby(lambda x: x['value'])
gb.run()
