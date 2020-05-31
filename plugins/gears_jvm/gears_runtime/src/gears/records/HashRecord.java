package gears.records;

import java.util.HashMap;
import java.util.Map;

public class HashRecord extends BaseRecord {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Map<String, BaseRecord> hashMap;
	
	public Map<String, BaseRecord> getHashMap() {
		return hashMap;
	}

	public void setHashMap(Map<String, BaseRecord> hashMap) {
		this.hashMap = hashMap;
	}

	public HashRecord() {
		super();
		hashMap = new HashMap<String, BaseRecord>();
	}
	
	public void Set(String key, BaseRecord r) {
		hashMap.put(key, r);
	}
	
	public void SetMultiple(Object[] data) {
		for(int i = 0 ; i < data.length ; i+=2) {
			hashMap.put((String)data[i], (BaseRecord)data[i + 1]);
		}
	}
	
	public BaseRecord Get(String key) {
		return hashMap.get(key);
	}
}
