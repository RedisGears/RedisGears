package gears.operations;

import java.io.Serializable;

import gears.GearsFuture;

public interface AsyncFilterOperation<I extends Serializable> extends Serializable {

	public GearsFuture<Boolean> filter(I record);

}
