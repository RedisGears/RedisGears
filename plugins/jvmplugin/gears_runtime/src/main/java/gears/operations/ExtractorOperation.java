package gears.operations;

import java.io.Serializable;

public interface ExtractorOperation<I extends Serializable> extends Serializable {
	
	public String extract(I record) throws Exception;
	
}
