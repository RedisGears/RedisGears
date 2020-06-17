package gears;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

import gears.operations.AccumulateByOperation;
import gears.operations.AccumulateOperation;
import gears.operations.ExtractorOperation;
import gears.operations.FilterOperation;
import gears.operations.FlatMapOperation;
import gears.operations.ForeachOperation;
import gears.operations.MapOperation;
import gears.operations.OnRegisteredOperation;
import gears.readers.BaseReader;

public class GearsBuilder{
	private BaseReader reader;
	private long ptr;
	
	private native void init(String reader);
	
	private native void destroy();
	
	public native GearsBuilder map(MapOperation mapper);
	
	public native GearsBuilder flatMap(FlatMapOperation faltmapper);
	
	public native GearsBuilder foreach(ForeachOperation foreach);
	
	public native GearsBuilder filter(FilterOperation foreach);
	
	public native GearsBuilder accumulateBy(ExtractorOperation extractor, AccumulateByOperation accumulator);
	
	public native GearsBuilder localAccumulateBy(ExtractorOperation extractor, AccumulateByOperation accumulator);
	
	public native GearsBuilder accumulate(AccumulateOperation accumulator);
	
	public native GearsBuilder collect();
	
	public static native String hashtag();
	
	public static native String configGet(String key);

	public static native Object executeArray(String[] command);
	
	public static native void log(String msg, LogLevel level);
	
	public static native void jniTestHelper(long ctx);
	
	public static void log(String msg) {
		log(msg, LogLevel.NOTICE);
	}
	
	public static Object execute(String... command) {
		return executeArray(command);
	}
	
	@SuppressWarnings("unchecked")
	public static <t> t executeCommand(String... command) {
		return (t) executeArray(command);
	}
	
	public native GearsBuilder repartition(ExtractorOperation extractor);
	
	private native void innerRun(BaseReader reader);
	
	private native void innerRegister(BaseReader reader, ExecutionMode mode, OnRegisteredOperation onRegister);
	
	public void run(boolean jsonSerialize, boolean collect) {
		if(jsonSerialize) {
			this.map(r->{
				ObjectMapper objectMapper = new ObjectMapper();
				return objectMapper.writeValueAsString(r);
			});
		}
		if(collect) {
			this.collect();
		}
		innerRun(reader);
	}
	
	public void run() {
		run(true, true);
	}
	
	public void register() {
		register(ExecutionMode.ASYNC, null);
	}
	
	public void register(ExecutionMode mode) {
		register(mode, null);
	}
	
	public void register(ExecutionMode mode, OnRegisteredOperation onRegister) {
		innerRegister(reader, mode, onRegister);
	}
	
	public GearsBuilder(BaseReader reader) {
		this.reader = reader;
		init(reader.getName());
	}
	
	private static void onUnpaused(ClassLoader cl) throws IOException {
		Thread.currentThread().setContextClassLoader(cl);
	}
	
	private static byte[] serializeObject(Object o, GearsObjectOutputStream out, boolean reset) throws IOException {
		if(reset) {
			out.reset();
		}
		
		byte[] b = out.serializeObject(o);
		return b;
	}
	
	private static Object deserializeObject(byte[] bytes, GearsObjectInputStream in, boolean reset) throws IOException, ClassNotFoundException {
		in.addData(bytes);
		Object o = in.readObject();
		return o;
	}
	
	private static String getStackTrace(Throwable e) {
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));
		return writer.toString();
	}
	
	private static void jniCallHelper(long ctx){
		jniTestHelper(ctx);
	}
		
	@Override
	protected void finalize() throws Throwable {
		destroy();
	}
}
