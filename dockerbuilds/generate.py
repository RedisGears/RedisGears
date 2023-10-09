import argparse
import os
import sys

import jinja2

# REDIS_VERSION is is s3://redismodules/redis-stack/dependencies
# it is not the version of redis, but the version of the pre-compiled package
defaults = {
    "OS_VERSION": "22.04",
    "OS_FAMILY": "ubuntu",
    "REDIS_VERSION": "7.2.1-1",
    "PLATFORM": "x86_64",
    "PUBLISH": False,
    "V8_VERSION": "default",
    "V8_UPDATE_HEADERS": "no",
    "MSRV": "",
}

p = argparse.ArgumentParser()
for d in defaults.keys():
    if defaults[d] in [True, False]:
        p.add_argument(
            f"--{d.lower()}", help=d, action="store_true", default=defaults[d]
        )
    else:
        p.add_argument(f"--{d.lower()}", help=d, default=defaults[d])
p.add_argument("-d", "--dest", help="Destination file", default="Dockerfile.generated")
p.add_argument("-s", "--src", help="Source template", default="dockerfile.debian.tmpl")

args = p.parse_args()
ctx = {k: getattr(args, k.lower()) for k in defaults.keys()}
if args.publish is False:
    del ctx["PUBLISH"]

try:
    t = open(args.src).read()
except FileNotFoundError:
    t = open(os.path.join(os.getcwd(), "dockerbuilds", args.src)).read()

tmpl = jinja2.Environment(loader=jinja2.FileSystemLoader("dockerbuilds")).from_string(t)
destfile = os.path.join(os.path.dirname(__file__), args.dest)
with open(destfile, "w") as f:
    f.write(tmpl.render(ctx))
