# This PythonReader will generate 42 records by creating a generator function
def createGenerator(count=6379):
    def generator():
        nonlocal count
        for x in range(count):
            yield x
    return generator

gb = GB('PythonReader')
gb.run(createGenerator(count=42))
