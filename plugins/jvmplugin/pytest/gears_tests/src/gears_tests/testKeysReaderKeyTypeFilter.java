package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testKeysReaderKeyTypeFilter {
	public static void main() throws IOException {
		KeysReader reader = new KeysReader();
		reader.setKeyTypes(new String[]{"list"});
		new GearsBuilder(reader).
		repartition(r->"counter").
		foreach(r->{
			GearsBuilder.execute("incr", "counter");
		}).
		register();
	}
}
