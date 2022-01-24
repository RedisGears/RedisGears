package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysOnlyReader;

public class testKeysOnlyReaderPattern {
	public static void main() {
		KeysOnlyReader reader = new KeysOnlyReader(10000, "pref1:*");
		GearsBuilder.CreateGearsBuilder(reader).run();
	}
}
