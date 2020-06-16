package gears_tests;

import java.util.Arrays;
import java.util.Comparator;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.BaseReader;
import gears.readers.CommandReader;

public class testCommandReaderBasic {
	
	private static void RegisterBuilder(BaseReader reader, ExecutionMode mode) {
		new GearsBuilder(reader).
		flatMap(r->{
			Object[] res = Arrays.stream((Object[])r).map(a->{
				return new String((byte[])a);
			}).
			sorted(Comparator.reverseOrder()).toArray();
			return Arrays.asList(res);
		}).
		register(mode);
	}
	
	public static void main() throws Exception {
		RegisterBuilder(new CommandReader().setTrigger("test1"), ExecutionMode.ASYNC);
		RegisterBuilder(new CommandReader().setTrigger("test2"), ExecutionMode.SYNC);
		RegisterBuilder(new CommandReader().setTrigger("test3"), ExecutionMode.ASYNC_LOCAL);
	}
}
