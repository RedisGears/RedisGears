package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testMap {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		map(r->{
			return ((KeysReaderRecord)r).getStringVal();
		}).run();
	}
}
