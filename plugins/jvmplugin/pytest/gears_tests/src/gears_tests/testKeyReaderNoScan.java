package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testKeyReaderNoScan {
	public static void main() {
		KeysReader reader = new KeysReader().setNoScan(true).setPattern("key");
		new GearsBuilder(reader).
		filter(r->{
			return !((KeysReaderRecord)r).getKey().equals("key");
		}).
		run();
	}
}
