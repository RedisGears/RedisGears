package gears_tests;

import java.util.ArrayList;
import java.util.List;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testKeysReaderCommandsOption {
	public static void main() {
		KeysReader reader = new KeysReader();
		
		// sync test
		reader.setCommands(new String[] {"get"});
		reader.setEventTypes(new String[] {"keymiss"});
		reader.setPattern("x");
		GearsBuilder.CreateGearsBuilder(reader).
		foreach(r->{
			byte[][] command = GearsBuilder.getCommand();
			List<String> commandStr = new ArrayList<String>();
			for(int i = 0 ; i < command.length ; ++i) {
				commandStr.add(new String(command[i]));
			}
			GearsBuilder.overrideReply(commandStr);
		}).
		register(ExecutionMode.SYNC);
		
		// async_local test
		reader.setCommands(new String[] {"get"});
		reader.setEventTypes(new String[] {"keymiss"});
		reader.setPattern("y");
		GearsBuilder.CreateGearsBuilder(reader).
		foreach(r->{
			byte[][] command = GearsBuilder.getCommand();
			List<String> commandStr = new ArrayList<String>();
			for(int i = 0 ; i < command.length ; ++i) {
				commandStr.add(new String(command[i]));
			}
			GearsBuilder.overrideReply(commandStr);
		}).
		register(ExecutionMode.ASYNC_LOCAL);
		
		// async test
		reader.setCommands(new String[] {"get"});
		reader.setEventTypes(new String[] {"keymiss"});
		reader.setPattern("z");
		GearsBuilder.CreateGearsBuilder(reader).
		foreach(r->{
			byte[][] command = GearsBuilder.getCommand();
			List<String> commandStr = new ArrayList<String>();
			for(int i = 0 ; i < command.length ; ++i) {
				commandStr.add(new String(command[i]));
			}
			GearsBuilder.overrideReply(commandStr);
		}).
		register(ExecutionMode.ASYNC);
	}
}
