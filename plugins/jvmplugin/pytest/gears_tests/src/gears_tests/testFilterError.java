package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testFilterError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		filter(r->{
			throw new Exception("Filter Error");
		}).run();
	}
}
