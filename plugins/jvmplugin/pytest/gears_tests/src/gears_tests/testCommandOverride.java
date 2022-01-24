package gears_tests;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandOverrider;

public class testCommandOverride {
	public static void main() {
		CommandOverrider reader = new CommandOverrider().setCommand("hset").setPrefix("h");
		
		GearsBuilder.CreateGearsBuilder(reader).
		map(r->{
			long currTime = System.currentTimeMillis();
			List<String> command = new ArrayList<String>();
			for(Object elem : r) {
				command.add(new String((byte[])elem));
			}
			command.add("time");
			command.add(Long.toString(currTime));
			command = command.subList(1, command.size());
			Object res = GearsBuilder.callNextArray(command.toArray(new String[0]));
			return (Serializable)res;
		}).register(ExecutionMode.SYNC);
		
	}
}
