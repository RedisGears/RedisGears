env_prefix=oss

if [ -n "$1" ]
then
 env_prefix=$1
fi

shift

./run_tests.sh $env_prefix --use-valgrind --vg-suppressions ./../leakcheck.supp $@

