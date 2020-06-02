package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface AccumulateOperation extends Serializable {

	public Serializable accumulate(Serializable accumulator, Serializable record) throws Exception;
	
}
