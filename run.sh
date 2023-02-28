# Run under release
#redis-server --loadmodule ./target/release/libredisgears.so ./target/release/libredisgears_v8_plugin.so

# Run under debug
redis-server --loadmodule ./target/debug/libredisgears.so ./target/debug/libredisgears_v8_plugin.so --enable-debug-command yes

# Run some locally-built redis server with the debug library.
#../redis/src/redis-server --loadmodule ./target/debug/libredisgears.so ./target/debug/libredisgears_v8_plugin.so --enable-debug-command yes

# Run the redis-server built locally with the module under GDB for immediate debugging.
#gdb --args ../redis/src/redis-server --loadmodule ./target/debug/libredisgears.so ./target/debug/libredisgears_v8_plugin.so --enable-debug-command yes

# Run valgrind with the debug module and redis-server in $PATH.
#valgrind --leak-check=full redis-server --loadmodule ./target/debug/libredisgears.so ./target/debug/libredisgears_v8_plugin.so
