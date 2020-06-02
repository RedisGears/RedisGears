package gears.operations;

import java.io.Serializable;

import gears.records.BaseRecord;

public interface MapOperation extends Serializable {
	
	public Serializable map(Serializable record) throws Exception;
	
}
