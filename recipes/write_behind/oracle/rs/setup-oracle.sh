#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/rafi-behind_oracle-1/recipes/write_behind/oracle/rs/setup-oracle.sh

set -e

BRANCH=rafi-behind_oracle-1

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe

/opt/recipe/oracle/install-oracle-docker
/opt/recipe/oracle/install-oracle-client

echo "127.0.0.1 oracle" >> /etc/hosts

/opt/recipe/oracle/rs/create-db

exit 0
