package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testRegistersOnPrefix {
	public static void main() throws IOException {
		KeysReader reader = new KeysReader();
		reader.setPattern("pref1:*");
		new GearsBuilder(reader).
		filter(r->{
			return ((KeysReaderRecord)r).getType() != KeysReaderRecord.REDISMODULE_KEYTYPE_EMPTY;
		}).
		map(r->{
			KeysReaderRecord kr = ((KeysReaderRecord)r); 
			String key = kr.getKey();
			String newKey = "pref2:" + key.split(":")[1];
			kr.setKey(newKey);
			return kr;
		}).
		repartition(r->{
			return ((KeysReaderRecord)r).getKey();
		}).
		foreach(r->{
			KeysReaderRecord kr = ((KeysReaderRecord)r);
			GearsBuilder.execute("set", kr.getKey(), kr.getStringVal());
		}).
		register();
	}
}
