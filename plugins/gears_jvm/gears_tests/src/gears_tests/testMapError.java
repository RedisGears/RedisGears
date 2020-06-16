package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testMapError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		map(r->{
			throw new Exception("Map Error");
		}).run();
	}
}
