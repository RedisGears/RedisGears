package gears.readers;

import java.io.Serializable;

import gears.operations.OnRegisteredOperation;

public abstract class BaseReader implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private ExecutionMode mode;
	
	private OnRegisteredOperation onRegistered;

	public BaseReader(ExecutionMode mode, OnRegisteredOperation onRegistered) {
		this.setMode(mode);
		this.onRegistered = onRegistered;
	}
	
	public abstract String GetName();

	public ExecutionMode getMode() {
		return mode;
	}

	public BaseReader setMode(ExecutionMode mode) {
		this.mode = mode;
		return this;
	}

	public OnRegisteredOperation getOnRegistered() {
		return onRegistered;
	}

	public BaseReader setOnRegistered(OnRegisteredOperation onRegistered) {
		this.onRegistered = onRegistered;
		return this;
	}
	
	
}
