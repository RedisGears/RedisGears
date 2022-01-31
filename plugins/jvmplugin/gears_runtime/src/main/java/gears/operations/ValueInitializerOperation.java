package gears.operations;

import java.io.Serializable;

public interface ValueInitializerOperation<R extends Serializable> extends Serializable{
	
	public R getInitialValue();
	
}
