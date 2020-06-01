package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface AccumulateByOperation extends Serializable {

	public BaseRecord Accumulateby(String key, BaseRecord accumulator, BaseRecord record) throws Exception;
	
}
