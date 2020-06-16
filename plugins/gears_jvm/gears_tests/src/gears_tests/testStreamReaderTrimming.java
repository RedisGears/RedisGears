package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.StreamReader;

public class testStreamReaderTrimming {
	public static void main() throws IOException {
		StreamReader reader = new StreamReader();
		reader.setPattern("stream").setBatchSize(3);
		new GearsBuilder(reader).
		foreach(r->{
			GearsBuilder.execute("incr", "NumOfElements");
		}).
		register();
	}
}
