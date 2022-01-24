package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testNullAccumulator {
	public static void main() throws IOException {
		KeysReader reader = new KeysReader(); 
		GearsBuilder.CreateGearsBuilder(reader).accumulate(null);
	}
}
