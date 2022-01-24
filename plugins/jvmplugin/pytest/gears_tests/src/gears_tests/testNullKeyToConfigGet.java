package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;

public class testNullKeyToConfigGet {
	public static void main() throws IOException {
		String s = null;
		GearsBuilder.configGet(s);
	}
}
