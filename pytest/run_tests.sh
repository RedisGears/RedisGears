set -x

env_prefix=oss
module_suffix=so

if [ -n "$1" ]
then
 env_prefix=$1
fi

if [ "$env_prefix" != "oss" ]
then
 module_suffix=zip
fi

shift

echo "no cluster on "$env_prefix
RLTest --clear-logs --module ../redistar.so --env $env_prefix $@
echo "cluster mode, 1 shard"
RLTest --clear-logs --module ../redistar.$module_suffix --env $env_prefix-cluster --shards-count 1 $@
echo "cluster mode, 2 shards"
RLTest --clear-logs --module ../redistar.$module_suffix --env $env_prefix-cluster --shards-count 2 $@
echo "cluster mode, 3 shards"
RLTest --clear-logs --module ../redistar.$module_suffix --env $env_prefix-cluster --shards-count 3 $@
