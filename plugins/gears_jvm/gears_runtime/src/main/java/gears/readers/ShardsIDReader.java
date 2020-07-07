package gears.readers;

/**
 * A reader that return, on each shard, a single record which is the current shard id.
 * This reader is good when one want to run some operation on each shard.
 *
 */
public class ShardsIDReader extends BaseReader<byte[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String getName() {
		return "ShardIDReader";
	}
	
}
