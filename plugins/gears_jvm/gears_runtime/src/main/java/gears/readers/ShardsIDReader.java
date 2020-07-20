package gears.readers;

/**
 * Implementation of a reader that returns a single record that is the current
 * shard's ID.
 * <p>
 * This reader is useful for executing operations on all shards.
 *
 * @since 1.0
 */
public class ShardsIDReader extends BaseReader<byte[]> {

	private static final long serialVersionUID = 1L;

	@Override
	public String getName() {
		return "ShardIDReader";
	}

}
