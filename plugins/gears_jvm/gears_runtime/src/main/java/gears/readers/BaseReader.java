package gears.readers;

import java.io.Serializable;

public abstract class BaseReader implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BaseReader() {}
	
	public abstract String GetName();
}
