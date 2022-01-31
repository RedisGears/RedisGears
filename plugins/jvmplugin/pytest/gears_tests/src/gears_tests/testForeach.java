package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testForeach {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		foreach(r->{ 
			((KeysReaderRecord)r).getHashVal().put("test", "test");
		}).
		run();
	}
}
