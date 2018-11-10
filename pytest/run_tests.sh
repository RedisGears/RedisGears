echo "no cluster"
RLTest --clear-logs --module ../redistar.so
echo "cluster mode, 1 shard"
RLTest --clear-logs --module ../redistar.so --env oss-cluster --shards-count 1
echo "cluster mode, 2 shards"
RLTest --clear-logs --module ../redistar.so --env oss-cluster --shards-count 2
echo "cluster mode, 3 shards"
RLTest --clear-logs --module ../redistar.so --env oss-cluster --shards-count 3
