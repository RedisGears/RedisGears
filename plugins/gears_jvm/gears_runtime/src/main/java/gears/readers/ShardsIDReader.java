package gears.readers;

import java.io.Serializable;

import gears.GearsBuilder;

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
