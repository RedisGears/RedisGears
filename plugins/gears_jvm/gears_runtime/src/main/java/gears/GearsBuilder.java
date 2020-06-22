package gears;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
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
import gears.operations.OnUnregisteredOperation;
import gears.operations.ValueInitializerOperation;
import gears.readers.BaseReader;

public class GearsBuilder<T extends Serializable>{
	private BaseReader<T> reader;
	private long ptr;
	
	private native void init(String reader);
	
	private native void destroy();
	
	public native <I extends Serializable> GearsBuilder<I> map(MapOperation<T, I> mapper);
	
	public native <I extends Serializable> GearsBuilder<I> flatMap(FlatMapOperation<T, I> faltmapper);
	
	public native GearsBuilder<T> foreach(ForeachOperation<T> foreach);
	
	public native GearsBuilder<T> filter(FilterOperation<T> foreach);
	
	public native <I extends Serializable> GearsBuilder<I> accumulateBy(ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator);
	
	public <I extends Serializable> GearsBuilder<I> accumulateBy(ValueInitializerOperation<I> valueInitializer, ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator){
		return this.accumulateBy(extractor, new AccumulateByOperation<T, I>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public I accumulateby(String k, I a, T r) throws Exception {
				if(a == null) {
					a = valueInitializer.getInitialValue();
				}
				return accumulator.accumulateby(k, a, r);
			}
			
		});
	}
	
	public native <I extends Serializable> GearsBuilder<I> localAccumulateBy(ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator);
	
	public native <I extends Serializable> GearsBuilder<I> accumulate(AccumulateOperation<T, I> accumulator);
	
	public <I extends Serializable> GearsBuilder<I> accumulate(I initialValue, AccumulateOperation<T, I> accumulator){
		return this.accumulate(new AccumulateOperation<T, I>() {
			private static final long serialVersionUID = 1L;

			@Override
			public I accumulate(I a, T r) throws Exception {
				if(a == null) {
					a = initialValue;
				}
				return accumulator.accumulate(a, r);
			}
			
		});
	}
	
	public native GearsBuilder<T> collect();
	
	public GearsBuilder<Integer> count(){
		return this.accumulate(0, (a, r)-> 1 + a);
	}
	
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
	
	public native GearsBuilder<T> repartition(ExtractorOperation<T> extractor);
	
	private native void innerRun(BaseReader<T> reader);
	
	private native void innerRegister(BaseReader<T> reader, ExecutionMode mode, OnRegisteredOperation onRegister, OnUnregisteredOperation onUnregistered);
	
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
		register(ExecutionMode.ASYNC, null, null);
	}
	
	public void register(ExecutionMode mode) {
		register(mode, null, null);
	}
	
	public void register(ExecutionMode mode, OnRegisteredOperation onRegister, OnUnregisteredOperation onUnregistered) {
		innerRegister(reader, mode, onRegister, onUnregistered);
	}
	
	public GearsBuilder(BaseReader<T> reader) {
		this.reader = reader;
		init(reader.getName());
	}
	
	public static <I extends Serializable> GearsBuilder<I> CreateGearsBuilder(BaseReader<I> reader) {
		return new GearsBuilder<I>(reader);
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
	
	private static String recordToString(Serializable record){
		return record.toString();
	}
		
	@Override
	protected void finalize() throws Throwable {
		destroy();
	}
}
