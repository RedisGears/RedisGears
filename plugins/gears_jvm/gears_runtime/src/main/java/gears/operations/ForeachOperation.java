package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface ForeachOperation extends Serializable {

	public void foreach(Serializable record) throws Exception;
	
}
