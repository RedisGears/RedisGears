package gears;
/**
 * Execution Modes:
 * 
 * 	ASYNC - execution will run asynchronously and be distributed on all the shards.
 *  ASYNC_LOCAL - execution will run asynchronously but only on the current shard that generate the event.
 *  SYNC -execution will run synchronously only on the same shard that generate the event.
 */
public enum ExecutionMode {
	ASYNC, ASYNC_LOCAL, SYNC
}
