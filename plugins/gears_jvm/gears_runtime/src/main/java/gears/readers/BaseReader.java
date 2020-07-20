package gears.readers;

import java.io.Serializable;

/**
 * Base reader class - all readers must extends this class
 *
 * @since 1.0
 * @param <T> when a {@link gears.GearsBuilder} is created this type will be the
 * first record type that will pass through the builder
 */
public abstract class BaseReader<T extends Serializable>{
	private static final long serialVersionUID = 1L;

	public BaseReader() {}

	/**
	 * Returns the reader's name - must be implemented by the reader
	 *
	 * @return A String representing the reader's name
	 */
	public abstract String getName();

}
