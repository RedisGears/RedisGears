package gears.operations;

import java.io.Serializable;

public interface FlatMapOperation<I extends Serializable, R extends Serializable> extends Serializable {

	public Iterable<R> flatmap(I record) throws Exception;
	
}
