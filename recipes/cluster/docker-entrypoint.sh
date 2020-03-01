#!/bin/sh
set -e

# Allow user-mounted config
if [ ! -f config.sh ]
then
    cp docker-config.sh config.sh
fi

bash ./create-cluster start
printf 'yes' | bash ./create-cluster create
redis-cli --cluster call 127.0.0.1:30001 RG.REFRESHCLUSTER
bash ./create-cluster tailall