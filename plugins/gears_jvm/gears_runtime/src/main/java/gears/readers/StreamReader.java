package gears.readers;

import gears.operations.OnRegisteredOperation;

public class StreamReader extends BaseReader {
	
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

	public StreamReader(ExecutionMode mode, OnRegisteredOperation onRegistered) {
		super(mode, onRegistered);
		this.pattern = "*";
		this.startId = "0-0";
		this.batchSize = 1;
		this.duration = 0;
		this.failurePolicy = FailurePolicy.CONTINUE;
		this.failureRertyInterval = 5000;
		this.trimStream = false;
	}

	@Override
	public String GetName() {
		// TODO Auto-generated method stub
		return "StreamReader";
	}
	
	public String getPattern() {
		return pattern;
	}

	public StreamReader setPattern(String pattern) {
		this.pattern = pattern;
		return this;
	}

	public String getStartId() {
		return startId;
	}

	public StreamReader setStartId(String startId) {
		this.startId = startId;
		return this;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public StreamReader setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public int getDuration() {
		return duration;
	}

	public StreamReader setDuration(int duration) {
		this.duration = duration;
		return this;
	}

	public FailurePolicy getFailurePolicy() {
		return failurePolicy;
	}

	public StreamReader setFailurePolicy(FailurePolicy failurePolicy) {
		this.failurePolicy = failurePolicy;
		return this;
	}

	public int getFailureRertyInterval() {
		return failureRertyInterval;
	}

	public StreamReader setFailureRertyInterval(int failureRertyInterval) {
		this.failureRertyInterval = failureRertyInterval;
		return this;
	}

	public boolean isTrimStream() {
		return trimStream;
	}

	public StreamReader setTrimStream(boolean trimStream) {
		this.trimStream = trimStream;
		return this;
	}

}
