package gears_tests;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandReader;
import gears.readers.KeysReader;

public class testKeysReaderCommandsOptionBadArgsError {
	public static void main() {
		
		CommandReader cr = new CommandReader().setTrigger("bad_command");
		GearsBuilder.CreateGearsBuilder(cr).
		foreach(r->{
			KeysReader reader = new KeysReader();
			reader.setCommands(new String[] {"temp"});
			reader.setEventTypes(new String[] {"keymiss"});
			reader.setPattern("x");
			GearsBuilder.CreateGearsBuilder(reader).
			foreach(r1->{
				GearsBuilder.overrideReply("-no such key");
			}).
			register(ExecutionMode.SYNC);
		}).register(ExecutionMode.SYNC);
		
		cr = new CommandReader().setTrigger("unallow_command");
		GearsBuilder.CreateGearsBuilder(cr).
		foreach(r->{
			KeysReader reader = new KeysReader();
			reader.setCommands(new String[] {"multi"});
			reader.setEventTypes(new String[] {"keymiss"});
			reader.setPattern("x");
			GearsBuilder.CreateGearsBuilder(reader).
			foreach(r1->{
				GearsBuilder.overrideReply("-no such key");
			}).
			register(ExecutionMode.SYNC);
		}).register(ExecutionMode.SYNC);
		
		cr = new CommandReader().setTrigger("unknown_keys_command");
		GearsBuilder.CreateGearsBuilder(cr).
		foreach(r->{
			KeysReader reader = new KeysReader();
			reader.setCommands(new String[] {"xread"});
			reader.setEventTypes(new String[] {"keymiss"});
			reader.setPattern("x");
			GearsBuilder.CreateGearsBuilder(reader).
			foreach(r1->{
				GearsBuilder.overrideReply("-no such key");
			}).
			register(ExecutionMode.SYNC);
		}).register(ExecutionMode.SYNC);
		
		cr = new CommandReader().setTrigger("blocking_command");
		GearsBuilder.CreateGearsBuilder(cr).
		foreach(r->{
			KeysReader reader = new KeysReader();
			reader.setCommands(new String[] {"blpop"});
			reader.setEventTypes(new String[] {"keymiss"});
			reader.setPattern("x");
			GearsBuilder.CreateGearsBuilder(reader).
			foreach(r1->{
				GearsBuilder.overrideReply("-no such key");
			}).
			register(ExecutionMode.SYNC);
		}).register(ExecutionMode.SYNC);
		
	}
}
