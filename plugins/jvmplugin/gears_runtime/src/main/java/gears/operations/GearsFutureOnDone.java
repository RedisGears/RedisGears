package gears.operations;

import java.io.Serializable;

public interface GearsFutureOnDone<I extends Serializable> {
	public void OnDone(I record) throws Exception;
	public void OnFailed(String error) throws Exception;
}
