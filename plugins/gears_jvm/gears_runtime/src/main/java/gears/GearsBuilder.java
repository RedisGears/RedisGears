package gears;

import java.io.IOException;
import gears.operations.AccumulateByOperation;
import gears.operations.AccumulateOperation;
import gears.operations.ExtractorOperation;
import gears.operations.FilterOperation;
import gears.operations.ForeachOperation;
import gears.operations.MapOperation;
import gears.readers.BaseReader;

public class GearsBuilder{
	
	private BaseReader reader;
	private long ptr;
	
	private native void init(String reader);
	
	private native void destroy();
	
	public native GearsBuilder map(MapOperation mapper);
	
	public native GearsBuilder flatMap(MapOperation mapper);
	
	public native GearsBuilder foreach(ForeachOperation foreach);
	
	public native GearsBuilder filter(FilterOperation foreach);
	
	public native GearsBuilder accumulateBy(ExtractorOperation extractor, AccumulateByOperation accumulator);
	
	public native GearsBuilder localAccumulateBy(ExtractorOperation extractor, AccumulateByOperation accumulator);
	
	public native GearsBuilder accumulate(AccumulateOperation accumulator);
	
	public native GearsBuilder collect();
	
	public static native Object executeArray(String[] command);
	
	public static Object execute(String... command) {
		return executeArray(command);
	}
	
	public native GearsBuilder repartition(ExtractorOperation extractor);
	
	private native void innerRun(BaseReader reader);
	
	private native void innerRegister(BaseReader reader);
	
	public void run() {
		innerRun(reader);
	}
	
	public void register() {
		innerRegister(reader);
	}
	
	public GearsBuilder(BaseReader reader) {
		this.reader = reader;
		init(reader.getName());
	}
	
	private static void onUnpaused(ClassLoader cl) throws IOException {
		Thread.currentThread().setContextClassLoader(cl);
	}
	
	private static byte[] serializeObject(Object o, GearsObjectOutputStream out) throws IOException {
		return out.serializeObject(o);
	}
	
	private static Object deserializeObject(byte[] bytes, GearsObjectInputStream in) throws IOException, ClassNotFoundException {
		in.addData(bytes);
		Object o = in.readObject();
		return o;
	}
	
	@Override
	protected void finalize() throws Throwable {
		destroy();
	}
}
