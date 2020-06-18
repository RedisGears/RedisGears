package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface MapOperation<I extends Serializable, R extends Serializable> extends Serializable {
	
	public R map(I record) throws Exception;
	
}
