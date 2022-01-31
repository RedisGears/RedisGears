package gears_tests;

import java.io.IOException;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandReader;

public class testSessionUpgradeWithVersionAndDescription {
	
	/* This is a hack to change to version, never do it in reality */
	public static int VERSION = GearsBuilder.configGet("REQUESTED_VERSION") == null? 1 : Integer.parseInt(GearsBuilder.configGet("REQUESTED_VERSION"));
	public static String DESCRIPTION = "foo";
	
	public static void main() throws IOException {
		CommandReader reader = new CommandReader().setTrigger("test");
		GearsBuilder.CreateGearsBuilder(reader).
		map(r->"OK").
		register(ExecutionMode.SYNC);
	}
}
