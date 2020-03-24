# This PythonReader will generate 6379 records
def generator():
    for x in range(6379):
        yield x

gb = GB('PythonReader')
gb.run(generator)
