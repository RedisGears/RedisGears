package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface AccumulateByOperation extends Serializable {

	public Serializable accumulateby(String key, Serializable accumulator, Serializable record) throws Exception;
	
}
