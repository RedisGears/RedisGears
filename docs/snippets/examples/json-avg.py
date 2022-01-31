import json

gb = GB()
gb.map(lambda r: json.loads(execute('json.get', r['key'], '$'))) # read the json
gb.map(lambda x: x['age']) # extrac age field
gb.avg() # calculate avg
gb.run("doc*") # run the execution on all keys start with doc*