package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testFilter {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		filter(r->{
			return ((KeysReaderRecord)r).getKey().equals("x");
		}).run();
	}
}
