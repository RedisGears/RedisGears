package gears_tests;

import java.io.IOException;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testUnregisterCallback {
	public static void main() throws IOException {
		KeysReader reader = new KeysReader();
		GearsBuilder.CreateGearsBuilder(reader).
		register(ExecutionMode.ASYNC, null, ()->{
			GearsBuilder.execute("FLUSHALL");
		});
	}
}
