package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testAccumulateError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		accumulate((a, r)->{
			throw new Exception("Accumulate Error");
		}).run();
	}
}
