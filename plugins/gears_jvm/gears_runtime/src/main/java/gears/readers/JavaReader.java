package gears.readers;

import java.io.Serializable;

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
