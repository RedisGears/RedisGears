package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface ForeachOperation extends Serializable {

	public void Foreach(BaseRecord record) throws Exception;
	
}
