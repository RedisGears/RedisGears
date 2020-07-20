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
 * A RedisGears function builder
 * <p>
 * RedisGears functions describe steps in a data processing flow that always
 * begins with one of the {@link gears.readers}. The reader can generate any
 * number of input {@link gears.records} as its output. These records are used
 * as input for the next step in the flow, in which the records can be operated
 * upon in some manner and then output. Multiple {@link gears.operations} can
 * be added to the flow, with each transforming its input records in some
 * meaningful way to one or more output records.
 * <p>
 * To create a builder use the following example:
 * <pre>
 * {@code
 * BaseReader reader = ...; // initialize reader
 * GearsBuilder builder = GearsBuilder.CreateGearsBuilder(reader)
 * }
 * </pre>
 * @since 1.0
 * @param <T> the record type passed to the function
 */
public class GearsBuilder<T extends Serializable>{
	private BaseReader<T> reader;
	private long ptr;

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Notify that a class loader has freed so we will free native structs
	 * holding the class loader.
	 * @param prt a pointer to native data
	 */
	protected static native void classLoaderFinalized(long prt);

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Called on GearsBuilder to creation to initialize native data.
	 *
	 * @param reader the reader name which the builder was created with
	 * @param desc execution description (could be null)
	 */
	private native void init(String reader, String desc);

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Called when builder is freed to free native data.
	 */
	private native void destroy();

	/**
	 * Add a map operation to the function
	 * <p>
	 * The following example maps each record to its record value:
	 * <pre>
	 * {@code
	 * GearsBuilder.CreateGearsBuilder(reader).
	 *     map(r->{
     *         return r.getStringVal();
	 *     });
	 * }
	 * </pre>
	 *
	 * @param <I> the template type of the returned builder
	 * @param mapper the map operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native <I extends Serializable> GearsBuilder<I> map(MapOperation<T, I> mapper);

	/**
	 * Add a flatmap operation to the function
	 * <p>
	 * The operation must return an Iterable object. RedisGears iterates over
	 * the elements in the Iterable object and passesthem one by one to the
	 * function.
	 * <p>
	 * Example:
	 * <pre>
	 * {@code
	 * GearsBuilder.CreateGearsBuilder(reader).
	 *     flatMap(r->{
	 *         return r.getListVal();
	 *   	});
	 * }
	 * </pre>
	 *
	 * @param <I> the template type of the returned builder
	 * @param faltmapper the flatmap operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native <I extends Serializable> GearsBuilder<I> flatMap(FlatMapOperation<T, I> faltmapper);

	/**
	 * Add a foreach operation to the function
	 * <p>
	 * Example:
	 * <pre>
	 * {@code
	 * GearsBuilder.CreateGearsBuilder(reader).
	 *     foreach(r->{
	 *         r.getSetVal("test");
	 *     });
	 * }
	 * </pre>
	 *
	 * @param foreach the foreach operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native GearsBuilder<T> foreach(ForeachOperation<T> foreach);

	/**
	 * Add a filter operation to the function
	 * <p>
	 * The filter should return true if RedisGears should continue processing
	 * the record.
	 * <p>
	 * Example:
	 * <pre>
	 * {@code
	 * GearsBuilder.CreateGearsBuilder(reader).
	 *     filter(r->{
	 *         return !r.getKey().equals("UnwantedKey");
	 *      });
	 * }
	 * </pre>
	 *
	 * @param foreach the foreach operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native GearsBuilder<T> filter(FilterOperation<T> foreach);

	/**
	 * Add an accumulateBy operation to the pipe
	 * <p>
	 * The accumulate by take an extractor and an accumulator. The extractor
	 * extracts the data by which we should group records. The accumulator is
	 * the reduce function. The accumulator gets the group, accumulated data,
	 * and current record and returns a new accumulated data. The accumolator's
	 * initial value is null.
	 * <p>
	 * Example (counting the number of unique values):
	 * <pre>
	 * {@code
	 * GearsBuilder.CreateGearsBuilder(reader).
     *     accumulateBy(r->{
	 *         return r.getStringVal();
	 *     },(k, a, r)->{
	 *         Integer ret = null;
	 *     if(a == null) {
	 *         ret = 0;
	 *     } else {
	 *         ret = (Integer)a;
	 *     }
	 *         return ret + 1;
	 *     });
	 * }
	 * </pre>
	 *
	 * @param <I> the template type of the returned builder
	 * @param extractor the extractor operation
	 * @param accumulator the accumulator operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native <I extends Serializable> GearsBuilder<I> accumulateBy(ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator);

	/**
	 * A sugar syntax for the previous accumulateBy
	 * <p>
	 * It gets a valueInitiator callback so there is no need to check if the
	 * accumulator is null.
	 * <p>
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
	 * @param <I> he template type of the returned builder
	 * @param valueInitializer  an initiator operation, whenever the accumulated
	 * value is null we will use this function to initialize it.
	 * @param extractor the extractor operation
	 * @param accumulator the accumulator operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
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
	 * Same as accumulateBy, but performed locally on each shard
	 * <p>
	 * Example:
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
	 * @param <I> the template type of the returned builder
	 * @param extractor the extractor operation
	 * @param accumulator the accumulator operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native <I extends Serializable> GearsBuilder<I> localAccumulateBy(ExtractorOperation<T> extractor, AccumulateByOperation<T, I> accumulator);

	/**
	 * A many to one mapper
	 * <p>
	 * Reduces the records  to a single record. The initial accumulator object
	 * is null (same as for accumulateBy).
	 * <p>
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
	 * @param <I> the template type of the returned builder
	 * @param accumulator the accumulate operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native <I extends Serializable> GearsBuilder<I> accumulate(AccumulateOperation<T, I> accumulator);

	/**
	 * A sugar syntax for the previous accumulateBy
	 * <p>
	 * It that gets the initial value as parameter so there is no need to check
	 * if the accumulated object is null.
	 * <p>
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
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
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
	 * Collect all the records to the shard that started the execution
	 * @return GearsBuilder with the same template type as the input builder, notice that the return object might be the same as the previous.
	 */
	public native GearsBuilder<T> collect();

	/**
	 * Add a count operation to the pipe
	 * <p>
	 * The operation returns a single record, which is the number of records it
	 * processed.
	 *
	 * @return {@link #GearsBuilder} with a new template type (Integer), notice that the return object might be the same as the previous.
	 */
	public GearsBuilder<Integer> count(){
		return this.accumulate(0, (a, r)-> 1 + a);
	}

	/**
	 * Return a String that maps to the current shard according to the cluster's
	 * slots mapping
	 * <p>
	 * This is useful when creating a key to ensure sure that the it resides on
	 * on the current shard.
	 * <p>
	 * Example:
	 * <pre>
	 * {@code
	 * execute("set", String.format("key{%s}", GearsBuilder.hashtag()), "1");
	 * }
	 * </pre>
	 *
	 * In the example the "{}" makes sure the hash slot will be calculated on
	 * the String located between the "{}" and using the hashtag function we
	 * know that this String will be mapped to the current shard.
	 *
	 * @return a String that, when hashed, is mapped to the current shard
	 */
	public static native String hashtag();

	/**
	 * Return a configuration value of a given key
	 * <p>
	 * The configuration value can be set when loading the module or by using
	 * the <a href="https://oss.redislabs.com/redisgears/commands.html#rgconfigset">
	 * RG.CONFIGSET command</a>.
	 *
	 * @param key the configuration key for which to return the value
	 * @return the configuration value of the given key
	 */
	public static native String configGet(String key);

	/**
	 * Execute a Redis command
	 * @param command the command the execute
	 * @return the command's reply (a String or an array or Strings, depending
	 * on the command)
	 */
	public static native Object executeArray(String[] command);

	/**
	 * Write a log message to the redis log file
	 *
	 * @param msg the message to write
	 * @param level the log level
	 */
	public static native void log(String msg, LogLevel level);

	/**
	 * <em>Internal use</em>
	 * <p>
	 * For performance increasment.
	 *
	 * @param ctx the context
	 */
	public static native void jniTestHelper(long ctx);

	/**
	 * Write a log message with log level Notice to Redis log file
	 * @param msg the message to write
	 */
	public static void log(String msg) {
		log(msg, LogLevel.NOTICE);
	}

	/**
	 * Execute a command on Redis
	 * @param command the command to execute
	 * @return the command's reply (a String or an array or Strings, depending
	 * on the command)
	 */
	public static Object execute(String... command) {
		return executeArray(command);
	}

	/**
	 * Add a repartition operation to the function
	 * <p>
	 * The repartition moves the records between the shards according to
	 * the extracted data.
	 *<p>
	 * Example (repartition by value):
	 * <pre>{@code
	 * 		GearsBuilder.CreateGearsBuilder(reader).
     *   	repartition(r->{
	 *   		return r.getStringVal();
	 *   	});
	 * }</pre>
	 *
	 * @param extractor the extractor operation
	 * @return {@link #GearsBuilder} with a new template type (note that the
	 * returned type may be different than the previous).
	 */
	public native GearsBuilder<T> repartition(ExtractorOperation<T> extractor);

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Called by run function to start the native code
	 *
	 * @param reader the reader object
	 */
	private native void innerRun(BaseReader<T> reader);

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Called by the register function to start the native code.
	 *
	 * @param reader the reader object
	 * @param mode the registration mode
	 * @param onRegister onRegister callback
	 * @param onUnregistered onUnregister callback
	 */
	private native void innerRegister(BaseReader<T> reader, ExecutionMode mode, OnRegisteredOperation onRegister, OnUnregisteredOperation onUnregistered);

	/**
	 * Run the function
	 *
	 * @param jsonSerialize indicates whether or no to serialize the results to json before returning them
	 * @param collect indicates whether or not to collect the results from all the cluster before returning them
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

	/**
	 * Run the function with collection and JSON serialization of the results
	 */
	public void run() {
		run(true, true);
	}

	/**
	 * Register the function as an async registration (i.e execution will run
	 * asynchronously on the entire cluster)
	 */
	public void register() {
		register(ExecutionMode.ASYNC, null, null);
	}

	/**
	 * Register the pipe to be trigger on events
	 *
	 * @param mode the execution mode to use
	 */
	public void register(ExecutionMode mode) {
		register(mode, null, null);
	}

	/**
	 * Register the pipe to be trigger on events
	 *
	 * @param mode the execution mode to use
	 * @param onRegister register callback that will be called on each shard upon register
	 * @param onUnregistered unregister callback that will be called on each shard upon unregister
	 */
	public void register(ExecutionMode mode, OnRegisteredOperation onRegister, OnUnregisteredOperation onUnregistered) {
		innerRegister(reader, mode, onRegister, onUnregistered);
	}

	/**
	 * Creates a new GearsBuilder object
	 *
	 * @param reader the reader to use to create the builder
	 * @param desc the execution description
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
	 *
	 * @param reader the reader to use to create the builder
	 */
	public GearsBuilder(BaseReader<T> reader) {
		this(reader, null);
	}

	/**
	 * A static function to create GearsBuilder - use this to avoid type warnings
	 *
	 * @param <I> the template type of the returned builder, this type is defined by the reader.
	 * @param reader the pipe reader
	 * @param desc the execution description
	 * @return a new GearsBuilder
	 */
	public static <I extends Serializable> GearsBuilder<I> CreateGearsBuilder(BaseReader<I> reader, String desc) {
		return new GearsBuilder<>(reader, desc);
	}

	/**
	 * A static function to create GearsBuilder - use this to avoid type warnings.
	 *
	 * @param <I> the template type of the returned builder, this type is defined by the reader.
	 * @param reader the pipe reader
	 * @return a new GearsBuilder
	 */
	public static <I extends Serializable> GearsBuilder<I> CreateGearsBuilder(BaseReader<I> reader) {
		return new GearsBuilder<>(reader);
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Set the ContextClassLoader each time we start running an execution.
	 *
	 * @param cl class loader
	 * @throws IOException
	 */
	private static void onUnpaused(ClassLoader cl) {
		Thread.currentThread().setContextClassLoader(cl);
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Clean the ContextClassLoader when we finish to run an execution
	 * @throws IOException
	 */
	private static void cleanCtxClassLoader() throws IOException {
		Thread.currentThread().setContextClassLoader(null);
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Serialize an object.
	 * @param o
	 * @param out
	 * @param reset
	 * @return the serialized object
	 * @throws IOException
	 */
	private static byte[] serializeObject(Object o, GearsObjectOutputStream out, boolean reset) throws IOException {
		if(reset) {
			out.reset();
		}

		return out.serializeObject(o);
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Deserialize an object
	 * @param bytes
	 * @param in
	 * @param reset
	 * @return the deserialized object
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static Object deserializeObject(byte[] bytes, GearsObjectInputStream in, boolean reset) throws IOException, ClassNotFoundException {
		in.addData(bytes);
		return in.readObject();
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Return stack trace of an execution as String.
	 * @param e
	 * @return a stack trace as a String
	 */
	private static String getStackTrace(Throwable e) {
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));
		return writer.toString();
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * For performance boosting.
	 * @param ctx
	 */
	private static void jniCallHelper(long ctx){
		jniTestHelper(ctx);
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Return String representation of a record.
	 * @param record
	 * @return the String representation of a record
	 */
	private static String recordToString(Serializable record){
		return record.toString();
	}

	@Override
	protected void finalize() throws Throwable {
		destroy();
	}

	/**
	 * <em>Internal use</em>
	 * <p>
	 * Initiate a heap dump.
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
	 * <em>Internal use</em>
	 * <p>
	 * Force running GC.
	 * @throws IOException
	 */
	private static void runGC() {
		System.gc();
	}
}
