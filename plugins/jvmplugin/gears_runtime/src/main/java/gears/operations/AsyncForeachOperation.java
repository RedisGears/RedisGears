package gears.operations;

import java.io.Serializable;

import gears.GearsFuture;

public interface AsyncForeachOperation<I extends Serializable> extends Serializable {

	public GearsFuture<Serializable> foreach(I record) throws Exception;

}
