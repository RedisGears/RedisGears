package gears.operations;

import java.io.Serializable;

public interface FlatMapOperation extends Serializable {

	public Iterable flatmap(Serializable record) throws Exception;
	
}
