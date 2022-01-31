package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testBasicRegistration {
	public static void main() throws IOException {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		filter(r->{
			return !((KeysReaderRecord)r).getKey().equals("values");
		}).
		repartition(r->{
			return "values";
		}).
		foreach(r->{
			GearsBuilder.execute("lpush", "values", ((KeysReaderRecord)r).getStringVal());
		}).
		register();
	}
}
