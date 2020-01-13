#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/write_behind_snowflake_1/recipes/write_behind/snowflake/rs/setup-node.sh
# via: bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node-snowflake)
# to be executed on RS nodes

set -e

if [ ! -z $(command -v apt-get) ]; then
	apt-get -qq update
	apt-get install -yq git
elif [ ! -z $(command -v yum) ]; then
	yum install -yq git
fi

BRANCH=write_behind_snowflake_1
REPO_PATH=https://raw.githubusercontent.com/RedisGears/RedisGears/$BRANCH/recipes/write_behind/snowflake/rs

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git

ln -s /opt/RedisGears/recipes/write_behind /opt/recipe
cd /opt/RedisGears/recipes/write_behind
ln -s ../gears.py .

/opt/redislabs/bin/python2 /opt/recipe/snowflake/rs/install-modules.py --yaml /opt/recipe/snowflake/rs/redis-modules.yaml

/opt/recipe/snowflake/install-snowflake-client
/opt/recipe/snowflake/install-snowflake-python-client

exit 0
