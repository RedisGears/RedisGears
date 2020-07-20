package gears.readers;

import gears.records.KeysReaderRecord;

/**
 * Implementation of a reader that reads keys and their values
 * <p>
 * Only Redis String and Hashes values are currently supported. For other value
 * types, only the key's name will be returned.
 *
 * @since 1.0
 */
public class KeysReader extends BaseReader<KeysReaderRecord> {
	private static final long serialVersionUID = 1L;
	private String pattern;
	private boolean noScan;
	private boolean readValues;
	private String[] eventTypes;
	private String[] keyTypes;

	/**
	 * Create a new KeysReader object
	 */
	public KeysReader() {
		super();
		this.pattern = "*";
		this.noScan = false;
		this.readValues = true;
		this.eventTypes = null;
		this.keyTypes = null;
	}

	/**
	 * Create a new KeysReader object
	 *
	 * @param pattern the keys pattern to read
	 * @param noScan whether to scan the key space or just read the patter as is
	 * @param readValues whether to read keys values
	 * @param eventTypes if used with register, the event types to register on (most of the time its the command name)
	 * @param keyTypes if used with register, the key types to register on
	 */
	public KeysReader(String pattern, boolean noScan, boolean readValues, String[] eventTypes, String[] keyTypes) {
		super();
		this.pattern = pattern;
		this.noScan = noScan;
		this.readValues = readValues;
		this.eventTypes = eventTypes;
		this.keyTypes = keyTypes;
	}

	/**
	 * Create a new KeysReader object
	 *
	 * @param pattern the keys pattern to read
	 */
	public KeysReader(String pattern) {
		this(pattern, false, true, null, null);
	}

	/**
	 * Create a new KeysReader object
	 *
	 * @param pattern the keys pattern to read
	 * @param noScan whether to scan the key space or just read the patter as is
	 * @param readValues whether to read keys values
	 */
	public KeysReader(String pattern, boolean noScan, boolean readValues) {
		this(pattern, noScan, readValues, null, null);
	}

	/**
	 * Create a new KeysReader object
	 *
	 * @param prefix the keys prefix to read
	 * @param readValues whether to read keys values
	 * @param eventTypes if used with register, the event types to register on (most of the time its the command name)
	 * @param keyTypes if used with register, the key types to register on
	 */
	public KeysReader(String prefix, boolean readValues, String[] eventTypes, String[] keyTypes) {
		this(prefix, false, readValues, eventTypes, keyTypes);
	}

	/**
	 * Create a new KeysReader object
	 *
	 * @param prefix the keys prefix to read
	 * @param readValues whether to read keys values
	 */
	public KeysReader(String prefix, boolean readValues) {
		this(prefix, false, readValues, null, null);
	}

	@Override
	public String getName() {
		return "KeysReader";
	}

	/**
	 * Return the pattern of keys to read
	 * @return the pattern of keys to read
	 */
	public String getPattern() {
		return pattern;
	}

	/**
	 * Set the pattern of keys to read
	 * @param pattern the pattern of keys to read
	 * @return the reader
	 */
	public KeysReader setPattern(String pattern) {
		this.pattern = pattern;
		return this;
	}

	/**
	 * Indicate if scan is used
	 * @return true if scan is not used otherwise false
	 */
	public boolean isNoScan() {
		return noScan;
	}

	/**
	 * Set noScan value
	 * @param noScan true to indicate the reader to not use scan operation and just read the key as is.
	 * @return the reader
	 */
	public KeysReader setNoScan(boolean noScan) {
		this.noScan = noScan;
		return this;
	}

	/**
	 * Indicate whether to read the keys values
	 * @return true if reading the keys values otherwise false
	 */
	public boolean isReadValues() {
		return readValues;
	}

	/**
	 * Set reade values parameter
	 * @param readValues true if the reader is required to read values otherwise false
	 * @return the reader
	 */
	public KeysReader setReadValues(boolean readValues) {
		this.readValues = readValues;
		return this;
	}

	/**
	 * On register, returns the event type this reader register on
	 * @return the event type this reader register on
	 */
	public String[] getEventTypes() {
		return eventTypes;
	}

	/**
	 * On register, set the event type this reader register on
	 * @param eventTypes the event type this reader register on.
	 * @return the reader
	 */
	public KeysReader setEventTypes(String[] eventTypes) {
		this.eventTypes = eventTypes;
		return this;
	}

	/**
	 * On register, return the key types this reader register on
	 * @return the key types this reader register on
	 */
	public String[] getKeyTypes() {
		return keyTypes;
	}

	/**
	 * On register, set the key types this reader register on
	 * @param keyTypes - the key types this reader register on
	 * @return the reader
	 */
	public KeysReader setKeyTypes(String[] keyTypes) {
		this.keyTypes = keyTypes;
		return this;
	}
}
