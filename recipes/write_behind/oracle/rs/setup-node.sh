#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/rafi-behind_oracle-1/recipes/write_behind/oracle/rs/setup-node.sh

set -e

if [[ -z $ORACLE]]; then
	echo "Error: no ORACLE IP address given. Aborting."
	exit 1
fi
	
BRANCH=rafi_behind-oracle-1
REPO_PATH=https://raw.githubusercontent.com/RedisGears/RedisGears/$BRANCH/recipes/write_behind/oracle/rs
DIR=/opt/redisgears-setup

mkdir -p $DIR
wget -q -O $DIR/install-modules.py $REPO_PATH/install-modules.py
wget -q -O $DIR/redis-modules.yaml $REPO_PATH/redis-modules.yaml
/opt/redislabs/bin/python2 $DIR/install-modules.py --yaml $DIR/redis-modules.yaml

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git

ln -s /opt/RedisGears/recipes/write_behind /opt/recipe
cd recipe
ln -s ../gear.py

echo "$ORACLE oracle" >> /etc/hosts

/opt/recipe/oracle/install-oracle-client
/opt/recipe/oracle/install-oracle-python-client

exit 0
