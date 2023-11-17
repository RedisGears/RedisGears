package gears;

import java.io.Serializable;

import gears.operations.GearsFutureOnDone;
import gears.records.BaseRecord;

class FutureRecord<I extends Serializable> extends BaseRecord implements GearsFutureOnDone<I> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private transient long nativeAsyncRecordPtr;
	private transient int futureRecordType;

	private native void createAsyncRecord();
	private native void asyncRecordSetResult(I result);
	private native void asyncRecordSetError(String error);
	private native void asyncRecordFree();
	
	protected FutureRecord(GearsFuture<I> future) throws Exception {
		createAsyncRecord();
		future.setFutureCallbacks(this);
	}

	@Override
	public void OnDone(I record) throws Exception {
		synchronized (this) {
			asyncRecordSetResult(record);
		}
		
	}

	@Override
	public void OnFailed(String error) throws Exception {
		synchronized (this) {
			asyncRecordSetError(error);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		asyncRecordFree();
	}
	
}
