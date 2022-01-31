package gears_tests;

import java.io.IOException;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandReader;

public class testSessionUpgradeWithUpgradeData {
	
	/* This is a hack to change to version, never do it in reality */
	public static int VERSION = GearsBuilder.configGet("REQUESTED_VERSION") == null? 1 : Integer.parseInt(GearsBuilder.configGet("REQUESTED_VERSION"));
	public static String DESCRIPTION = "foo";
	
	public static String getUpgradeData() throws Exception {
		if (GearsBuilder.configGet("UPGRADE_EXCEPTION").equals("enabled")) {
			throw new Exception("Upgrade Exception");
		}
		return "some upgrade data";
	}
	
	public static void main() throws IOException {
		CommandReader reader = new CommandReader().setTrigger("get_upgrade_data");
		GearsBuilder.CreateGearsBuilder(reader).
		map(r-> {
			return GearsBuilder.getUpgradeData() != null ? GearsBuilder.getUpgradeData() : "no upgrade data";
		}).
		register(ExecutionMode.SYNC);
	}
}
