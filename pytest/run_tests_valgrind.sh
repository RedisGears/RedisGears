echo "no cluster valgrind"
RLTest --clear-logs --module ../redistar.so --use-valgrind --vg-suppressions ./../leakcheck.supp
echo "cluster mode, 1 shard, valgrind"
RLTest --clear-logs --module ../redistar.so --env oss-cluster --shards-count 1 --use-valgrind --vg-suppressions ./../leakcheck.supp
echo "cluster mode, 2 shards, valgrind"
RLTest --clear-logs --module ../redistar.so --env oss-cluster --shards-count 2 --use-valgrind --vg-suppressions ./../leakcheck.supp
echo "cluster mode, 3 shards, valgrind"
RLTest --clear-logs --module ../redistar.so --env oss-cluster --shards-count 3 --use-valgrind --vg-suppressions ./../leakcheck.supp
