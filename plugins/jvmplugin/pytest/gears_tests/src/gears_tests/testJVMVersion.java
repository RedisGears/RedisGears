package gears_tests;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandReader;
import gears.readers.KeysReader;

public class testJVMVersion {
	public static void main() {
		
		CommandReader cr = new CommandReader().setTrigger("jvm_version");
		GearsBuilder.CreateGearsBuilder(cr).
		map(r->{
            return System.getProperty("java.version");
		}).register(ExecutionMode.SYNC);
	}
}
