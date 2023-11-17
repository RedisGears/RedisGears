package gears.operations;

import java.io.Serializable;

public interface AccumulateByOperation<I extends Serializable, T extends Serializable> extends Serializable {

	public T accumulateby(String key, T accumulator, I record) throws Exception;
	
}
