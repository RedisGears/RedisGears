package gears.operations;

import java.io.Serializable;

public interface ForeachOperation<I extends Serializable> extends Serializable {

	public void foreach(I record) throws Exception;
	
}
