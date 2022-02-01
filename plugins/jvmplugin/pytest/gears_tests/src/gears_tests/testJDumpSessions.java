package gears_tests;

import java.io.IOException;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandReader;

public class testJDumpSessions {
	
	public static void main() throws IOException {
		CommandReader reader = new CommandReader().setTrigger("test");
		GearsBuilder.CreateGearsBuilder(reader).
		map(r-> {
			return "OK";
		}).
		register(ExecutionMode.SYNC);
	}
}
