package gears.records;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KeysReaderRecord extends BaseRecord {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final int REDISMODULE_KEYTYPE_EMPTY = 0;
	public static final int REDISMODULE_KEYTYPE_STRING = 1;
	public static final int REDISMODULE_KEYTYPE_LIST = 2;
	public static final int REDISMODULE_KEYTYPE_HASH = 3;
	public static final int REDISMODULE_KEYTYPE_SET = 4;
	public static final int REDISMODULE_KEYTYPE_ZSET = 5;
	public static final int REDISMODULE_KEYTYPE_MODULE = 6;
	public static final int REDISMODULE_KEYTYPE_STREAM = 7;
	
	private String key;
	private String event;
	private long type;
	private String stringVal;
	private Map<String,String> hashVal;
	private List<String> listVal;
	private Set<String> setVal;
	
	public KeysReaderRecord(String key, String event, boolean readVal, ByteBuffer buff) {
		this.key = key;
		this.event = event;
		if(readVal) {
			buff.order(ByteOrder.LITTLE_ENDIAN);
			this.type = buff.getLong();
			if(this.type == -1) {
				
			}
			if(type == REDISMODULE_KEYTYPE_HASH) {
				this.hashVal = new HashMap<String, String>();
				while(buff.position() < buff.capacity()) {
					long fieldSize = buff.getLong();
					byte[] dst = new byte[(int)fieldSize];
					buff.get(dst);
					String field = new String(dst);
					long valSize = buff.getLong();
					dst = new byte[(int)valSize];
					buff.get(dst);
					String val = new String(dst);
					this.hashVal.put(field, val);
				}
			}
			if(type == REDISMODULE_KEYTYPE_STRING) {
				long dataLen = buff.getLong();
				byte[] data = new byte[(int)dataLen];
				buff.get(data);
				this.stringVal = new String(data);					
			}
		}
	}

	public String getKey() {
		return key;
	}

	public String getEvent() {
		return event;
	}

	public long getType() {
		return type;
	}

	public String getStringVal() {
		return stringVal;
	}

	public Map<String, String> getHashVal() {
		return hashVal;
	}

	public List<String> getListVal() {
		return listVal;
	}

	public Set<String> getSetVal() {
		return setVal;
	}
	
}
