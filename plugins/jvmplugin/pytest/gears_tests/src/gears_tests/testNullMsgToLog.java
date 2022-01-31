package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;

public class testNullMsgToLog {
	public static void main() throws IOException {
		String s = null;
		GearsBuilder.log(s);
	}
}
