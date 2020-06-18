package gears.readers;

import java.io.Serializable;

public abstract class BaseReader<T extends Serializable>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BaseReader() {}
	
	public abstract String getName();

}
