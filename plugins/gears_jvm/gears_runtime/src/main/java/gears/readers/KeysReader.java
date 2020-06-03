package gears.readers;

import gears.operations.OnRegisteredOperation;

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

	public KeysReader() {
		super();
		this.pattern = "*";
		this.noScan = false;
		this.readValues = true;
		this.eventTypes = null;
		this.keyTypes = null;
	}
	
	public KeysReader(String pattern, boolean noScan, boolean readValues, String[] eventTypes, String[] keyTypes) {
		super();
		this.pattern = pattern;
		this.noScan = noScan;
		this.readValues = readValues;
		this.eventTypes = eventTypes;
		this.keyTypes = keyTypes;
	}
	
	public KeysReader(String pattern) {
		this(pattern, false, true, null, null);
	}
	
	public KeysReader(String pattern, boolean noScan, boolean readValues) {
		this(pattern, noScan, readValues, null, null);
	}
	
	public KeysReader(String prefix, boolean readValues, String[] eventTypes, String[] keyTypes) {
		this(prefix, false, readValues, eventTypes, keyTypes);
	}
	
	public KeysReader(String prefix, boolean readValues) {
		this(prefix, false, readValues, null, null);
	}

	@Override
	public String getName() {
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
}
