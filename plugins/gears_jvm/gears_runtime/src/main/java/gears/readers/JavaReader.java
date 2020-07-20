package gears.readers;

import java.io.Serializable;

/**
 * Implementation of a Java reader
 * <p>
 * In order to implement a custom reader one need to extend this class and
 * implement the {@code public Iterator<String> iterator()} function. RedisGears
 * will make sure to consume the returned iterator and pass the returned records
 * through the function.
 * <p>
 * Look at the {@link KeysOnlyReader}'s implementation for example.
 *
 * @since 1.0
 * @param <T> the returned record type
 */
public abstract class JavaReader<T extends Serializable> extends BaseReader<T> implements Iterable<T>, Serializable {
	private static final long serialVersionUID = 1L;

	@Override
	public final String getName() {
		return "JavaReader";
	}
}
