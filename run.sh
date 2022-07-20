#redis-server --loadmodule ./target/release/libredisgears.so ./target/release/libredisgears_v8_plugin.so
redis-server --loadmodule ./target/debug/libredisgears.so ./target/debug/libredisgears_v8_plugin.so --enable-debug-command yes
#valgrind --leak-check=full redis-server --loadmodule ./target/debug/libredisgears.so ./target/debug/libredisgears_v8_plugin.so
