#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/master/recipes/write_behind/oracle/rs/setup-node.sh
# via: ORACLE=<ip> bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node)
# to be executed on RS nodes

set -e

if [[ -z $ORACLE ]]; then
	echo "Error: no ORACLE IP address given. Aborting."
	exit 1
fi

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

ln -sf /opt/RedisGears/recipes/write_behind /opt/recipe
cd /opt/RedisGears/recipes/write_behind
ln -sf ../gears.py .

OSNICK=`/opt/redislabs/bin/python2 /opt/RedisGears/deps/readies/bin/platform --osnick`
[[ $OSNICK =~ 'rhel7' ]] && OSNICK='centos7'
if [[ $OSNICK != 'centos7' && $OSNICK != 'bionic' ]]; then
	echo "$OSNICK: incompatible platform. Aborting."
	exit 1
fi

MOD_DIR=/opt/recipe/rs
/opt/redislabs/bin/python2 $MOD_DIR/install-modules.py --no-bootstrap-check --yaml $MOD_DIR/redis-modules-$OSNICK.yaml

printf "\n$ORACLE oracle\n" >> /etc/hosts

/opt/recipe/oracle/install-oracle-client
/opt/recipe/oracle/install-oracle-python-client

exit 0
