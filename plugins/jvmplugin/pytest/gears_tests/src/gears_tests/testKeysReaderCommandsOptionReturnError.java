package gears_tests;

import java.util.ArrayList;
import java.util.List;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testKeysReaderCommandsOptionReturnError {
	public static void main() {
		KeysReader reader = new KeysReader();
		
		// sync test
		reader.setCommands(new String[] {"get"});
		reader.setEventTypes(new String[] {"keymiss"});
		reader.setPattern("x");
		GearsBuilder.CreateGearsBuilder(reader).
		foreach(r->{
			GearsBuilder.overrideReply("-no such key");
		}).
		register(ExecutionMode.SYNC);
	}
}
