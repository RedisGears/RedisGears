package gears_tests;

import java.util.ArrayList;
import java.util.Arrays;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.CommandReader;

public class testMainArguments {
	public static void main(String[] args) {
		CommandReader reader = new CommandReader().setTrigger("test");
		GearsBuilder.CreateGearsBuilder(reader)
		.flatMap(r->{
			return new ArrayList<String>(Arrays.asList(args));
		})
		.register(ExecutionMode.SYNC);
	}
}
