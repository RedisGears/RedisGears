package gears.operations;

import java.io.Serializable;

import gears.GearsFuture;

public interface AsyncMapOperation<I extends Serializable, R extends Serializable> extends Serializable {
	
	public GearsFuture<R> map(I record);
	
}
