package gears.readers;

import java.util.HashMap;

/**
 * Implementation of a reader that reads Redis Stream data
 * <p>
 * This reader returns records of type {@code HashMap<String, Object>} that
 * have the following keys:
 * <ol>
 * 	<li>key   - the key name as String
 * 	<li>id    - the current element id as String
 * 	<li>value - The current element value as {@code HashMap<String, byte[]>},
 *              the reason for using byte[] and not String is that there is not
 *              garentee that the values will be String and they might be blobs
 *              (images for example).
 * </ol>
 *
 * @since 1.0
 */
public class StreamReader extends BaseReader<HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	/**
	 * Policies for dealing with registered stream processing failure
	 */
	public enum FailurePolicy {
		/**
		 * Processing will continue for future events from the stream
		 */
		CONTINUE,
		/**
		 * Processing will not be done for future stream events
		 */
		ABORT,
		/**
		 * Retry the last batch after {@link #setFailureRertyInterval}
		 */
		RETRY
	}

	private String pattern;
	private String startId;
	private int batchSize;
	private int duration;
	private FailurePolicy failurePolicy;
	private int failureRertyInterval;
	private boolean trimStream;

	/**
	 * Create a StreamReader object
	 */
	public StreamReader() {
		super();
		this.pattern = "*";
		this.startId = "0-0";
		this.batchSize = 1;
		this.duration = 0;
		this.failurePolicy = FailurePolicy.CONTINUE;
		this.failureRertyInterval = 5000;
		this.trimStream = true;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "StreamReader";
	}

	/**
	 * Return the pattern to use to read the streams
	 * @return the pattern to use to read the streams
	 */
	public String getPattern() {
		return pattern;
	}

	/**
	 * Set the patter to use to read the streams
	 * <p>
	 * Note:
	 * <ul>
	 * 	<li>When use with Run operation, only exact match are supported
	 * 	<li>When used with register, currently only prefixes are supported
	 * </ul>
	 *
	 * @param pattern the patter to use to read the streams
	 * @return the reader
	 */
	public StreamReader setPattern(String pattern) {
		this.pattern = pattern;
		return this;
	}

	/**
	 * Return the start stream id from which to start reading
	 * @return the start stream id from which to start reading
	 */
	public String getStartId() {
		return startId;
	}

	/**
	 * Set the start stream id from which to start reading
	 * <p>
	 * This is applicable only when the reader is used with
	 * {@link gears.GearsBuilder#run()} or
	 * {@link gears.GearsBuilder#run(boolean, boolean)}.
	 *
	 * @param startId the start stream id from which to start reading
	 * @return the reader
	 */
	public StreamReader setStartId(String startId) {
		this.startId = startId;
		return this;
	}

	/**
	 * Return the batch size of the reader
	 * @return the batch size of the reader
	 */
	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * Set the batch size of the reader
	 * <p>
	 * The reader will wait until that number of records is available to read
	 * from the stream. This is applicable only when the reader is with used
	 * with {@link gears.GearsBuilder#register()}
	 *
	 * @param batchSize the batch size of the reader
	 * @return the reader
	 */
	public StreamReader setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Returns the reader duration
	 * @return the reader duration
	 */
	public int getDuration() {
		return duration;
	}

	/**
	 * Set the reader duration
	 * <p>
	 * When there is data in the stream for more tha 'duration' seconds, the
	 * reader will read them regrdless of the {@link #batchSize}. This is
	 * applicable only when the reader is with used with
	 * {@link gears.GearsBuilder#register()}
	 *
	 * @param duration the reader duration
	 * @return the reader
	 */
	public StreamReader setDuration(int duration) {
		this.duration = duration;
		return this;
	}

	/**
	 * The reader failure policy
	 * @return reader failure policy
	 */
	public FailurePolicy getFailurePolicy() {
		return failurePolicy;
	}

	/**
	 * Set the reader failure policy
	 * <p>
	 * This is applicable only when the reader is with used with
	 * {@link gears.GearsBuilder#register()}.
	 *
	 * @param failurePolicy the failure policy
	 * @return the reader
	 */
	public StreamReader setFailurePolicy(FailurePolicy failurePolicy) {
		this.failurePolicy = failurePolicy;
		return this;
	}

	/**
	 * Returns the retry interval of the current reader
	 * @return the retry interval of the current reader
	 */
	public int getFailureRertyInterval() {
		return failureRertyInterval;
	}

	/**
	 * Set the retry interval of the reader
	 * <p>
	 * The interval in seconds after retries are attempted. This is applicable
	 * only when the reader is with used with
	 * {@link gears.GearsBuilder#register()}, and
	 * {@link #setFailurePolicy(FailurePolicy)} with the
	 * {@link FailurePolicy#RETRY} policy.
	 *
	 * @param failureRertyInterval the retry interval of the reader
	 * @return the reader
	 */
	public StreamReader setFailureRertyInterval(int failureRertyInterval) {
		this.failureRertyInterval = failureRertyInterval;
		return this;
	}

	/**
	 * Indicate whether or not to trim the stream
	 * @return if true the stream will be trimmed on success or if
	 * 		   {@link FailurePolicy#CONTINUE} is used
	 */
	public boolean isTrimStream() {
		return trimStream;
	}

	/**
	 * Set the trim stream value
	 * <p>
	 * This is applicable only when the reader is with used
	 * with {@link gears.GearsBuilder#register()}.If true the stream will be
	 * trimmed on success or if {@link FailurePolicy#CONTINUE} is used.
	 * <p>
	 * Note: if trim stream is enabled, no other registration should register on
	 * this stream. Registering more then one execution on a stream with
	 * TrimStream enabled will result in undefined behavior.
	 *
	 * @param trimStream if true the stream will be trimmed on success or if
	 *                   {@link FailurePolicy#CONTINUE} is used
	 * @return           the reader
	 */
	public StreamReader setTrimStream(boolean trimStream) {
		this.trimStream = trimStream;
		return this;
	}

}
