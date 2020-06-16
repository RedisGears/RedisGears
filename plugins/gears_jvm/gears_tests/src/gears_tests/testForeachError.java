package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testForeachError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		foreach(r->{ 
			throw new Exception("Foreach Error");
		}).
		run();
	}
}
