package gears.readers;

import java.util.HashMap;

/**
 * A reader that reads Redis Stream data.
 * 
 * This Reader return a records of type {@code HashMap<String, Object>} that contains
 * the following keys:
 * 
 * 	1. key - the String key as String
 * 	2. id - the current element id as String
 * 	3. value - The current element value as {@code HashMap<String, byte[]>}, the reason
 *             of using byte[] and not String is that there is not garentee that 
 *             those values will be String and they might be blobs (images for example).
 *
 */
public class StreamReader extends BaseReader<HashMap<String, Object>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public enum FailurePolicy{
		CONTINUE, ABORT, RETRY
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
	 * Returns the patter to use to read the streams
	 * @return the patter to use to read the streams
	 */
	public String getPattern() {
		return pattern;
	}

	/**
	 * Set the patter to use to read the streams
	 * Notice:
	 * 	1. When use with Run operation, only exact match are supported
	 * 	2. When used with register, currently only prefixes are supported
	 * @param pattern the patter to use to read the streams
	 * @return the reader
	 */
	public StreamReader setPattern(String pattern) {
		this.pattern = pattern;
		return this;
	}

	/**
	 * Return the start stream id from which to start reading.
	 * @return the start stream id from which to start reading
	 */
	public String getStartId() {
		return startId;
	}

	/**
	 * Set the start stream id from which to start reading (only relevent when used with Run).
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
	 * Set the batch size of the reader, i.e the reader to read from the stream when the
	 * number of elements in the stream will reader this size. Only relevant when used with
	 * register.
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
	 * Set the reader duration, i.e if there is data in the stream for more than
	 * 'duration' seconds, the reader will read them regrdless of the batchSize
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
	 * Set the reader failure policy (relevant only for register).
	 * This policy is one of the following:
	 * 	1. CONTINUE - continue trigger more events
	 * 	2. ABORT - do not trigger any more events
	 *  3. RETRY - retry execute the last batch. The retry interval is
	 *             defined by FailureRertyInterval
	 * @param failurePolicy
	 * @return
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
	 * Set the retry interval of the reader (relevant only for register).
	 * This interval is relevant only if FailurePolicy is retry
	 * and indicate the interval in which perform the retries (in seconds).
	 * @param failureRertyInterval - the retry interval of the reader
	 * @return the reader
	 */
	public StreamReader setFailureRertyInterval(int failureRertyInterval) {
		this.failureRertyInterval = failureRertyInterval;
		return this;
	}

	/**
	 * Indicate whether or not to trim the stream
	 * @return if true the stream will be trimmed on success or if the FailurePolicy is continue
	 */
	public boolean isTrimStream() {
		return trimStream;
	}

	/**
	 * Set the trim stream value (relevant only for register).
	 * If true the stream will be trimmed on success or if the FailurePolicy is continue.
	 * Notice: if trim stream is enable, no other registration should register on this stream.
	 * 		   register more then one execution on a stream with TrimStream enable will cause
	 *         undefined behavior.
	 * 
	 * @param trimStream
	 * @return
	 */
	public StreamReader setTrimStream(boolean trimStream) {
		this.trimStream = trimStream;
		return this;
	}

}
