package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface AccumulateOperation extends Serializable {

	public BaseRecord Accumulate(BaseRecord accumulator, BaseRecord record) throws Exception;
	
}
