package gears.operations;

import java.io.Serializable;

public interface OnRegisteredOperation extends Serializable {
	public void onRegistered() throws Exception;
}
