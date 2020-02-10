#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/write_behind_1/recipes/gears/rs/setup-node.sh
# via: bash <(curl -fsSL https://cutt.ly/redisgears-setup-node)
# to be executed on RS nodes

set -e

if [ -z $(command -v git) ]; then
	if [ ! -z $(command -v apt-get) ]; then
		apt-get -qq update
		apt-get install -y git
	elif [ ! -z $(command -v yum) ]; then
		yum install -y git
	else
		echo "%make love"
		echo "Make:  Don't know how to make love.  Stop."
		exit 1
	fi
fi

if [[ $FORCE == 1 ]]; then
	rm -rf /opt/recipe /opt/RedisGears /opt/redislabs/lib/modules/python3
fi

BRANCH=write_behind_1

mkdir -p /opt
cd /opt
if [[ ! -d RedisGears ]]; then
	git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git
else
	cd RedisGears
	git pull
	cd ..
fi

ln -sf /opt/RedisGears/recipes/gears /opt/recipe
cd /opt/RedisGears/recipes/gears
ln -sf ../gears.py .

OSNICK=`/opt/redislabs/bin/python2 /opt/RedisGears/deps/readies/bin/platform --osnick`
[[ $OSNICK =~ 'rhel7' ]] && OSNICK='centos7'
if [[ $OSNICK != 'centos7' && $OSNICK != 'bionic' && $OSNICK != 'xenial' ]]; then
	echo "$OSNICK: incompatible platform. Aborting."
	exit 1
fi

MOD_DIR=/opt/recipe/rs
/opt/redislabs/bin/python2 $MOD_DIR/install-modules.py --no-bootstrap-check --yaml $MOD_DIR/redis-modules-$OSNICK.yaml

exit 0
