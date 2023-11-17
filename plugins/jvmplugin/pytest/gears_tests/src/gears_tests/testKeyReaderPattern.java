package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testKeyReaderPattern {
	public static void main() {
		KeysReader reader = new KeysReader().setPattern("pref1:*");
		new GearsBuilder(reader).
		map(r->{ 
			return ((KeysReaderRecord)r).getKey();
		}).
		run();
	}
}
