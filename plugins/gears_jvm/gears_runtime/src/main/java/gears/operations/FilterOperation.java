package gears.operations;

import java.io.Serializable;

public interface FilterOperation<I extends Serializable> extends Serializable {

	public boolean filter(I record) throws Exception;
	
}
