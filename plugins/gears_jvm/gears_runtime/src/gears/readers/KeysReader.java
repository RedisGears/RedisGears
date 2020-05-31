package gears.readers;

public class KeysReader extends BaseReader {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String pattern;
	private boolean noScan;
	private boolean readValues;
	private String[] eventTypes;
	private String[] keyTypes;
	private ExecutionMode mode;

	public KeysReader() {}
	
	public KeysReader(String pattern, ExecutionMode mode, boolean noScan, boolean readValues, String[] eventTypes, String[] keyTypes) {
		super();
		this.pattern = pattern;
		this.noScan = noScan;
		this.readValues = readValues;
		this.eventTypes = eventTypes;
		this.keyTypes = keyTypes;
		this.mode = mode;
	}
	
	public KeysReader(String pattern) {
		this(pattern, ExecutionMode.ASYNC, false, true, null, null);
	}
	
	public KeysReader(String pattern, boolean noScan, boolean readValues) {
		this(pattern, ExecutionMode.ASYNC, noScan, readValues, null, null);
	}
	
	public KeysReader(String prefix, ExecutionMode mode, boolean readValues, String[] eventTypes, String[] keyTypes) {
		this(prefix, mode, false, readValues, eventTypes, keyTypes);
	}
	
	public KeysReader(String prefix, ExecutionMode mode, boolean readValues) {
		this(prefix, mode, false, readValues, null, null);
	}

	@Override
	public String GetName() {
		return "KeysReader";
	}

	public String getPattern() {
		return pattern;
	}

	public KeysReader setPattern(String pattern) {
		this.pattern = pattern;
		return this;
	}

	public boolean isNoScan() {
		return noScan;
	}

	public KeysReader setNoScan(boolean noScan) {
		this.noScan = noScan;
		return this;
	}

	public boolean isReadValues() {
		return readValues;
	}

	public KeysReader setReadValues(boolean readValues) {
		this.readValues = readValues;
		return this;
	}

	public String[] getEventTypes() {
		return eventTypes;
	}

	public KeysReader setEventTypes(String[] eventTypes) {
		this.eventTypes = eventTypes;
		return this;
	}

	public String[] getKeyTypes() {
		return keyTypes;
	}

	public KeysReader setKeyTypes(String[] keyTypes) {
		this.keyTypes = keyTypes;
		return this;
	}

	public ExecutionMode getMode() {
		return mode;
	}

	public KeysReader setMode(ExecutionMode mode) {
		this.mode = mode;
		return this;
	}
}
