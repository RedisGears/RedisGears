# Filter out records where the key's length is less than four characters
gb = GB()
gb.filter(lambda x: len(x['key']) > 3)
gb.run()
