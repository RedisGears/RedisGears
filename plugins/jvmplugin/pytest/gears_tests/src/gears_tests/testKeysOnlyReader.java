package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysOnlyReader;

public class testKeysOnlyReader {
	public static void main() {
		KeysOnlyReader reader = new KeysOnlyReader();
		GearsBuilder.CreateGearsBuilder(reader).run();
	}
}
