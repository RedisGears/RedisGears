package gears;

import java.io.Serializable;

import gears.operations.GearsFutureOnDone;

public class GearsFuture<I extends Serializable> {
	private I result;
	private String error;
	private GearsFutureOnDone<I> callbacks;
	
	public GearsFuture() {}
	
	private void callOnDoneCallback() throws Exception {
		if(this.callbacks == null) {
			return;
		}
		if(result != null) {
			this.callbacks.OnDone(this.result);
			return;
		}
		if(error != null) {
			this.callbacks.OnFailed(error);
			return;
		}
	}
	
	public synchronized void setResult(I result) throws Exception {
		if(this.result != null || this.error != null) {
			throw new Exception("error/results is already set");
		}
		
		this.result = result;
		callOnDoneCallback();
	}
	
	public synchronized void setError(String error) throws Exception {
		if(this.result != null || this.error != null) {
			throw new Exception("error/results is already set");
		}
		
		this.error = error;
		callOnDoneCallback();
	}
	
	public synchronized void setFutureCallbacks(GearsFutureOnDone<I> callbacks) throws Exception {
		if(this.callbacks != null) {
			throw new Exception("onDone callback is already set");
		}
		this.callbacks = callbacks;
		callOnDoneCallback();
	}
}
