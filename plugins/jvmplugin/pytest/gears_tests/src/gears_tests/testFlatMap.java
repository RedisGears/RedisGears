package gears_tests;

import java.io.Serializable;
import java.util.Arrays;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testFlatMap {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		map(r->{
			return (Serializable) GearsBuilder.execute("lrange", 
					((KeysReaderRecord)r).getKey(),
					"0", "-1");
		}).
		flatMap(r->{
			return Arrays.asList((Object[])r);
		}).
		run();
	}
}
