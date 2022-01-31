package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testBasic {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).run();
	}
}
