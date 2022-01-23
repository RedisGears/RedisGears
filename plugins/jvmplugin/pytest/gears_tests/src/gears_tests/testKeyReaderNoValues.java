package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testKeyReaderNoValues {
	public static void main() {
		KeysReader reader = new KeysReader().setReadValues(false);
		new GearsBuilder(reader).
		filter(r->{ 
			return ((KeysReaderRecord)r).getStringVal() != null;
		}).
		run();
	}
}
