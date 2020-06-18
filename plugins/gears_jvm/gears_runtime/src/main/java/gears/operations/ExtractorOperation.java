package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface ExtractorOperation<I extends Serializable> extends Serializable {
	
	public String extract(I record) throws Exception;
	
}
