package gears_tests;

import java.io.IOException;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testOnRegisteredCallback {
	public static void main() throws IOException {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		register(ExecutionMode.ASYNC_LOCAL, (id)->{
			GearsBuilder.execute("set", String.format("registered{%s}", GearsBuilder.hashtag()), "1");
		}, null);
	}
}
