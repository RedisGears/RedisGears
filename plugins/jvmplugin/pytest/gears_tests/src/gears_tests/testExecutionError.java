package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testExecutionError {
	public static void main() throws Exception {
		throw new Exception("Test Exception");
	}
}
