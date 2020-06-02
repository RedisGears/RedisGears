package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface FilterOperation extends Serializable {

	public boolean filter(Serializable record) throws Exception;
	
}
