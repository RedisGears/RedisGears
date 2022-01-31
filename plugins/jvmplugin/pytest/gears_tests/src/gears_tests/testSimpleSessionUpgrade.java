package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.CommandReader;

public class testSimpleSessionUpgrade {
	public static void main() throws IOException {
		CommandReader reader = new CommandReader().setTrigger("test");
		GearsBuilder.CreateGearsBuilder(reader).
		collect().
		count().
		register();
	}
}
