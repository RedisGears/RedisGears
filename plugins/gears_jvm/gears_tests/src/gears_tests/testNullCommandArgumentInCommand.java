package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;

public class testNullCommandArgumentInCommand {
	public static void main() throws IOException {
		String s = null;
		GearsBuilder.execute(s);
	}
}
