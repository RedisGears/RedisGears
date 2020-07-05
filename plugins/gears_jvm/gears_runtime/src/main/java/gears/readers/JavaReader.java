package gears.readers;

import java.io.Serializable;
import java.util.Iterator;

/**
 * This reader is used to implement a reader in java.
 * In order to implement a costume reader one need to extend this class
 * and implement the 'public Iterator<String> iterator()' function.
 * RedisGears will make sure to consume the returned iterator and pass the
 * returned records through Gears pipe.
 * 
 * Look at the KeysOnlyReader implementation for example.
 *
 * @param <T> - the returned record type
 */
public abstract class JavaReader<T extends Serializable> extends BaseReader<T> implements Iterable<T>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public final String getName() {
		return "JavaReader";
	}
}
