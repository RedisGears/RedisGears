package gears.operations;

import java.io.Serializable;

public interface OnRegisteredOperation extends Serializable {
	public void OnRegistered() throws Exception;
}
