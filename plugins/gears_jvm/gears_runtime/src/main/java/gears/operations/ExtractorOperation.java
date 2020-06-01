package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface ExtractorOperation extends Serializable {
	
	public String Extract(BaseRecord record) throws Exception;
	
}
