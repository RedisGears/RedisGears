package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;

public class testNullArgumentInCommand {
	public static void main() throws IOException {
		String s = null;
		GearsBuilder.execute("get", s);
	}
}
