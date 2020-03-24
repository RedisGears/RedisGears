# Split KeyReader's record into two: one for key the other for value
gb = GB()
gb.flatmap(lambda x: [x['key'], x['value']])
gb.run()
