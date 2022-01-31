package gears_tests;

import java.io.Serializable;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testFlatmapError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		map(r->{
			return (Serializable) GearsBuilder.execute("lrange", 
					((KeysReaderRecord)r).getKey(),
					"0", "-1");
		}).
		flatMap(r->{
			throw new Exception("Flatmap Error");
		}).
		run();
	}
}
