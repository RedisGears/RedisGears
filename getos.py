import platform

dist = platform.dist()
dist = "-".join(x for x in dist)
print("%s-%s-%s" % (platform.system(), dist, platform.machine()))