package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface ExtractorOperation extends Serializable {
	
	public String extract(Serializable record) throws Exception;
	
}
