
def fread(fname, mode = 'rb'):
	with open(fname, mode) as file:
		return file.read()
