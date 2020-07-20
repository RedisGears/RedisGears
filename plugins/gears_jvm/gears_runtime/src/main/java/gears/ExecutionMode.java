package gears;
/**
 * RedisGears function execution modes
 */
public enum ExecutionMode {
	/**
	 * Execution will run asynchronously and be distributed on all the shards
	 */
	ASYNC,
	/**
	 * Execution will run asynchronously but only on the current shard that generate the event
	 */
	ASYNC_LOCAL,
	/**
	 * Execution will run synchronously only on the same shard that generate the event
	 */
	SYNC
}
