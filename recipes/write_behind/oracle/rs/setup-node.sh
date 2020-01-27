#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/master/recipes/write_behind/oracle/rs/setup-node.sh
# via: ORACLE=<ip> bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node)
# to be executed on RS nodes

set -e

if [[ -z $ORACLE ]]; then
	echo "Error: no ORACLE IP address given. Aborting."
	exit 1
fi
	
BRANCH=write_behind_1
REPO_PATH=https://raw.githubusercontent.com/RedisGears/RedisGears/$BRANCH/recipes/write_behind/oracle/rs

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git

ln -s /opt/RedisGears/recipes/write_behind /opt/recipe
cd /opt/RedisGears/recipes/write_behind
ln -s ../gears.py .

OSNICK=`/opt/redislabs/bin/python2 /opt/RedisGears/deps/readies/bin/platform --osnick`
if [[ $OSNICK != 'centos7' || $OSNICK != 'bionic' ]]; then
	echo "$OSNICK: incompatible platform. Aborting."
	exit 1
fi

MOD_DIR=/opt/recipe/oracle/rs
/opt/redislabs/bin/python2 $MOD_DIR/install-modules.py --no-bootstrap-check --yaml $MOD_DIR/redis-modules-$OSNICK.yaml

printf "\n$ORACLE oracle\n" >> /etc/hosts

/opt/recipe/oracle/install-oracle-client
/opt/recipe/oracle/install-oracle-python-client

exit 0
