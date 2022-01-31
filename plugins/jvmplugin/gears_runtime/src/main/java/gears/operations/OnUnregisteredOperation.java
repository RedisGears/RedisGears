package gears.operations;

import java.io.Serializable;

public interface OnUnregisteredOperation extends Serializable {
	public void onUnregistered() throws Exception;
}
