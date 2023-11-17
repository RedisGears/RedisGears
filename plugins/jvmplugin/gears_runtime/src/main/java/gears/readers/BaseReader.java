package gears.readers;

import java.io.Serializable;

/**
 * Base reader class, all readers must extends this class.
 *
 * @param <T> - When a GearsBuilder is created this type will be the first record
 * type that will pass through the builder pipe
 */
public abstract class BaseReader<T extends Serializable>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BaseReader() {}
	
	/**
	 * Returns the reader name, must be implemented by the reader.
	 * @return
	 */
	public abstract String getName();

}
