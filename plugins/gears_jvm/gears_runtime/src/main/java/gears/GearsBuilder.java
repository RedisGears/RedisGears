package gears;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.HotSpotDiagnosticMXBean;

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

/**
 * A RedisGears pipe builder. The data pass in the pipe
 * and transforms according to the operations in the pipe.
 * 
 * Each pipe starts with a reader, the reader is responsible of supply
 * data to the rest of the operation. 
 * To create a builder use the following example:
 * 
 * 	   BaseReader reader = ...; // initialize reader
 *     GearsBuilder builder = GearsBuilder.CreateGearsBuilder(reader)
 *
 * @param T - The current record type pass in the pipe 
 */
public class GearsBuilder<T extends Serializable>{
	private BaseReader<T> reader;
	private long ptr;
	
	/**
	 * Internal use
	 * 
	 * Notify that a class loader has freed so we will free native structs
	 * holding the class loader
	 * @param prt - pointer to native data
	 */
	protected static native void classLoaderFinalized(long prt);
	
	/**
	 * Internal use
	 * 
	 * Internal use, called on GearsBuilder to creation to initialize
	 * native data
	 * 
	 * @param reader - the reader name which the builder was created with
	 * @param desc - execution description (could be null)
	 */
	private native void init(String reader, String desc);
	
	/**
	 * Internal use
	 * 
	 * Called when builder is freed to free native data.
	 */
	private native void destroy();
	
	/**
	 * Add a map operation to the pipe.
	 * Example (map each record to the record value):
	 * <pre>{@code
	 * 		GearsBuilder.CreateGearsBuilder(reader).
	 * 		map(r->{
     *    		return r.getStringVal();
	 *   	})
	 * }</pre>
	 * 
	 * @param <I> The template type of the returned builder
	 * @param mapper - the map operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native <I extends Serializable> GearsBuilder<I> map(MapOperation<T, I> mapper);
	
	/**
	 * Add a flatmap operation to the pipe. the operation must return an Iterable
	 * object. RedisGears iterate over the element in the Iterable object and pass
	 * them one by one in the pipe.
	 * Example:
	 * <pre>{@code 		
	 * 		GearsBuilder.CreateGearsBuilder(reader).
	 *   	flatMap(r->{
	 *   		return r.getListVal();
	 *   	}); 
	 * }</pre>
	 * 
	 * @param <I> The template type of the returned builder
	 * @param faltmapper - the faltmap operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native <I extends Serializable> GearsBuilder<I> flatMap(FlatMapOperation<T, I> faltmapper);
	
	/**
	 * Add a foreach operation to the pipe.
	 * 
	 * Example:
	 * <pre>{@code 		
	 * 		GearsBuilder.CreateGearsBuilder(reader).
	 *  	foreach(r->{
	 *   		r.getSetVal("test");
	 * 		});
	 * }</pre>
	 * 
	 * @param foreach - the foreach operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native GearsBuilder<T> foreach(ForeachOperation<T> foreach);
	
	/**
	 * Add a filter operation to the pipe.
	 * The filter should return true if RedisGears should continue process the record
	 * and otherwise false
	 * 
	 * Example:
	 * <pre>{@code 
	 *  	GearsBuilder.CreateGearsBuilder(reader).
	 *   	filter(r->{
	 *   		return !r.getKey().equals("UnwantedKey");
	 *   	});
	 * }</pre>
	 * 
	 * @param foreach - the foreach operation
	 * @return - GearsBuilder with the same template type as the input builder, notice that the return object might be the same as the previous.
	 */
	public native GearsBuilder<T> filter(FilterOperation<T> foreach);
	
	/**
	 * Add an accumulateBy operation to the pipe.
	 * the accumulate by take an extractor and an accumulator.
	 * The extractor extracts the data by which we should perform the group by
	 * The accumulate is the reduce function. The accumulator gets the group, accumulated data, and current record
	 * and returns a new accumulated data. The initial value of the accumulator is null.
	 * 
	 * Example (counting the number of unique values):
	 * <pre>{@code
	 * 		GearsBuilder.CreateGearsBuilder(reader).
     *   	accumulateBy(r->{
	 *   		return r.getStringVal();
	 *   	},(k, a, r)->{
	 *   		Integer ret = null;
	 *   		if(a == null) {
	 *   			ret = 0;
	 *   		}else {
	 *   			ret = (Integer)a;
	 *   		}
	 *   		return ret + 1;
	 *   	});
	 * }</pre>
	 * 
	 * @param <I> - The template type of the returned builder
	 * @param extractor - the extractor operation
	 * @param accumulator - the accumulator operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native <I extends Serializable> GearsBuilder<I> accumulateBy(ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator);
	
	/**
	 * A sugar syntax for the previous accumulateBy that gets a valueInitiator callback
	 * so there is no need to check if the accumulator is null.
	 * 
	 * Example (same example of counting the number of unique values):
	 * <pre>{@code 		
	 * 		GearsBuilder.CreateGearsBuilder(reader).
     *    	accumulateBy(()->{
	 *   		return 0;
	 *   	},r->{
	 *   		return r.getStringVal();
	 *   	},(k, a, r)->{
	 *   		return a + 1;
	 *   	});
	 * }</pre>
	 * 
	 * @param <I> - The template type of the returned builder 
	 * @param valueInitializer - an initiator operation, 
	 * whenever the accumulated values is null we will use this function to initialize it.
	 * @param extractor - the extractor operation
	 * @param accumulator - the accumulator operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
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
	
	/**
	 * Same as accumulate by but performed locally on each shard (without moving
	 * data between shards).
	 * 
	 * Example:
	 * 
	 * <pre>{@code
	 * 		GearsBuilder.CreateGearsBuilder(reader).
	 *   	localAccumulateBy(r->{
	 *   		return r.getStringVal();
	 *   	},(k, a, r)->{
	 *   		Integer ret = null;
	 *   		if(a == null) {
	 *   			ret = 0;
	 *   		}else {
	 *   			ret = (Integer)a;
	 *   		}
	 *   		return ret + 1;
	 *   	});
	 * }</pre>
	 * 
	 * @param <I> - The template type of the returned builder
	 * @param extractor - the extractor operation
	 * @param accumulator - the accumulator operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native <I extends Serializable> GearsBuilder<I> localAccumulateBy(ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator);
	
	/**
	 * A many to one mapped, reduce the record in the pipe to a single record.
	 * The initial accumulator object is null (same as for accumulateBy)
	 * 
	 * Example (counting the number of records):
	 * <pre>{@code		
	 * 		GearsBuilder.CreateGearsBuilder(reader).
     *    	accumulate((a, r)->{
	 *   		Integer ret = null;
	 *   		if(a == null) {
	 *   			ret = 1;
	 *   		}else {
	 *   			ret = (Integer)a;
	 *   		}
	 *   		return ret + 1;
	 *   	});
	 * }</pre>
	 * 
	 * @param <I> - The template type of the returned builder
	 * @param accumulator - the accumulate operation
	 * @return - GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native <I extends Serializable> GearsBuilder<I> accumulate(AccumulateOperation<T, I> accumulator);
	
	/**
	 * A sugar syntax for the previous accumulateBy that gets the initial value as parameter
	 * so there is no need to check if the accumulated object is null.
	 * 
	 * Example (counting the number of records):
	 * <pre>{@code
	 *  	GearsBuilder.CreateGearsBuilder(reader).
	 *   	accumulate(0, (a, r)->{
	 *   		return a + 1;
	 *   	});
	 * }</pre>
	 * 
	 * @param <I> - The template type of the returned builder
	 * @param initialValue - the initial value of the accumulated object
	 * @param accumulator - the accumulate operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
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
	
	/**
	 * Collects all the records to the shard that started the execution.
	 * @return GearsBuilder with the same template type as the input builder, notice that the return object might be the same as the previous.
	 */
	public native GearsBuilder<T> collect();
	
	/**
	 * Add a count operation to the pipe, the operation returns a single record
	 * which is the number of records in the pipe.
	 * @return GearsBuilder with a new template type (Integer), notice that the return object might be the same as the previous.
	 */
	public GearsBuilder<Integer> count(){
		return this.accumulate(0, (a, r)-> 1 + a);
	}
	
	/**
	 * Returns a String that maps to the current shard according to the cluster slot mapping.
	 * It is very useful when there is a need to create a key and make sure that the key
	 * reside on the current shard.
	 * 
	 *  Example:
	 *  	execute("set", String.format("key{%s}", GearsBuilder.hashtag()), "1")
	 *  
	 *  In the following example the "{}" make sure the hslot will be calculated on the String located between the
	 *  "{}" and using the  hashtag function we know that this String will be mapped to the current shard.
	 *  
	 * @return String reside on the current shard when calculating hslot
	 */
	public static native String hashtag();
	
	/**
	 * Return a configuration value of a given key. the configuration value 
	 * can be set when loading the module or using RG.CONFIGSET command (https://oss.redislabs.com/redisgears/commands.html#rgconfigset)
	 * 
	 * @param key - the configuration key for which to return the value
	 * @return the configuration value of the given key
	 */
	public static native String configGet(String key);

	/**
	 * Execute a command on Redis
	 * 
	 * @param command - the command the execute
	 * @return the command result (could be a simple String or an array or Strings depends on the command)
	 */
	public static native Object executeArray(String[] command);
	
	/**
	 * Write a log message to the redis log file
	 * 
	 * @param msg - the message to write
	 * @param level - the log level
	 */
	public static native void log(String msg, LogLevel level);

	/**
	 * Internal use for performance increasment.
	 * 
	 * @param ctx
	 */
	public static native void jniTestHelper(long ctx);
	
	/**
	 * Write a log message with log level Notice to Redis log file
	 * @param msg - the message to write
	 */
	public static void log(String msg) {
		log(msg, LogLevel.NOTICE);
	}
	
	/**
	 * Execute a command on Redis
	 * 
	 * @param command - the command to execute
	 * @return the command result (could be a simple String or an array or Strings depends on the command)
	 */
	public static Object execute(String... command) {
		return executeArray(command);
	}
	
	/**
	 * Add a repartition operation to the operation pipe.
	 * The repartition moves the records between the shards according to
	 * the extracted data.
	 * 
	 * Example (repartition by value):
	 * <pre>{@code 
	 * 		GearsBuilder.CreateGearsBuilder(reader).
     *   	repartition(r->{
	 *   		return r.getStringVal();
	 *   	});
	 * }</pre>
	 * 
	 * @param extractor - the extractor operation
	 * @return GearsBuilder with a new template type, notice that the return object might be the same as the previous.
	 */
	public native GearsBuilder<T> repartition(ExtractorOperation<T> extractor);

	/**
	 * Internal use, called by run function to start the native code
	 * @param reader - the reader object
	 */
	private native void innerRun(BaseReader<T> reader);
	
	/**
	 * Internal use, called by the register function to start the native code.
	 * 
	 * @param reader - the reader object
	 * @param mode - the registration mode
	 * @param onRegister - onRegister callback
	 * @param onUnregistered - onUnregister callback
	 */
	private native void innerRegister(BaseReader<T> reader, ExecutionMode mode, OnRegisteredOperation onRegister, OnUnregisteredOperation onUnregistered);
	
	/**
	 * Runs the current built pipe
	 * 
	 * @param jsonSerialize - indicate whether or no to serialize the results to json before returning them
	 * @param collect - indicate whether or not to collect the results from all the cluster before returning them
	 */
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

	/*
	 * Runs the current built pipe, collects all the results from all the shards
	 * before returning them and serialize them as a json object.
	 */
	public void run() {
		run(true, true);
	}
	
	/*
	 * Register the pipe as an async registration (i.e execution will run asynchronously on the entire cluster)
	 */
	public void register() {
		register(ExecutionMode.ASYNC, null, null);
	}
	
	/**
	 * Register the pipe to be trigger on events
	 * @param mode - the execution mode to use (ASYNC/ASYNC_LOCAL/SYNC)
	 */
	public void register(ExecutionMode mode) {
		register(mode, null, null);
	}
	
	/**
	 * Register the pipe to be trigger on events
	 * @param mode - the execution mode to use (ASYNC/ASYNC_LOCAL/SYNC)
	 * @param onRegister - register callback that will be called on each shard upon register
	 * @param onUnregistered - unregister callback that will be called on each shard upon unregister
	 */
	public void register(ExecutionMode mode, OnRegisteredOperation onRegister, OnUnregisteredOperation onUnregistered) {
		innerRegister(reader, mode, onRegister, onUnregistered);
	}
	
	/**
	 * Creates a new GearsBuilde object
	 * @param reader - the reader to use to create the builder
	 * @param desc - the execution description
	 */
	public GearsBuilder(BaseReader<T> reader, String desc) {
		if(reader == null) {
			throw new NullPointerException("Reader can not be null");
		}
		this.reader = reader;
		init(reader.getName(), desc);
	}
	
	/**
	 * Creates a new GearsBuilde object
	 * @param reader - the reader to use to create the builder
	 */
	public GearsBuilder(BaseReader<T> reader) {
		this(reader, null);
	}
	
	/**
	 * A static function to create GearsBuilder. We use this to avoid type warnings.
	 * @param <I> - The template type of the returned builder, this type is defined by the reader.
	 * @param reader - The pipe reader
	 * @param desc - the execution description
	 * @return a new GearsBuilder
	 */
	public static <I extends Serializable> GearsBuilder<I> CreateGearsBuilder(BaseReader<I> reader, String desc) {
		return new GearsBuilder<>(reader, desc);
	}
	
	/**
	 * A static function to create GearsBuilder. We use this to avoid type warnings.
	 * @param <I> - The template type of the returned builder, this type is defined by the reader.
	 * @param reader - The pipe reader
	 * @return a new GearsBuilder
	 */
	public static <I extends Serializable> GearsBuilder<I> CreateGearsBuilder(BaseReader<I> reader) {
		return new GearsBuilder<>(reader);
	}
	
	/**
	 * Internal use, set the ContextClassLoader each time we start running an execution
	 * @param cl - class loader
	 * @throws IOException
	 */
	private static void onUnpaused(ClassLoader cl) {
		Thread.currentThread().setContextClassLoader(cl);
	}
	
	/**
	 * Internal use, clean the ContextClassLoader when we finish to run an execution
	 * @throws IOException
	 */
	private static void cleanCtxClassLoader() throws IOException {
		Thread.currentThread().setContextClassLoader(null);
	}
	
	/**
	 * Internal use, serialize an object.
	 * @param o
	 * @param out
	 * @param reset
	 * @return
	 * @throws IOException
	 */
	private static byte[] serializeObject(Object o, GearsObjectOutputStream out, boolean reset) throws IOException {
		if(reset) {
			out.reset();
		}
		
		return out.serializeObject(o);
	}
	
	/**
	 * Internal user, deserialize an object
	 * @param bytes
	 * @param in
	 * @param reset
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static Object deserializeObject(byte[] bytes, GearsObjectInputStream in, boolean reset) throws IOException, ClassNotFoundException {
		in.addData(bytes);
		return in.readObject();
	}
	
	/**
	 * Internal use, return stack trace of an execution as String.
	 * @param e
	 * @return
	 */
	private static String getStackTrace(Throwable e) {
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));
		return writer.toString();
	}
	
	/**
	 * Internal use, for performance boosting
	 * @param ctx
	 */
	private static void jniCallHelper(long ctx){
		jniTestHelper(ctx);
	}
	
	/**
	 * Internal use, return String representation of a record.
	 * @param record
	 * @return
	 */
	private static String recordToString(Serializable record){
		return record.toString();
	}
		
	@Override
	protected void finalize() throws Throwable {
		destroy();
	}
	
	/**
	 * Internal use, initiate a heap dump.
	 * @param dir
	 * @param filePath
	 * @throws IOException
	 */
	private static void dumpHeap(String dir, String filePath) throws IOException {
	    log("Dumping heap into: " + dir + '/' + filePath);
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();
	    HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
	      server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
	    mxBean.dumpHeap(dir + '/' + filePath, true);
	}
	
	/**
	 * Internal use, force runnint GC.
	 * @throws IOException
	 */
	private static void runGC() {
		System.gc();		
	}
}
