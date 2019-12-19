#!/bin/bash

for dir in */ ; do
	d=$(basename $dir)
	t=gears-$d.tar
	tar cf $t $d/* --transform "s,$d/,gears_$d/,"
	tar rf $t gears.py --transform "s,^,gears_$d/,"
	gzip $t
done
