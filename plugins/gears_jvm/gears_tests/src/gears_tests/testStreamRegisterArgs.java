package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.StreamReader;
import gears.readers.StreamReader.FailurePolicy;

public class testStreamRegisterArgs {
	public static void main() throws IOException {
		StreamReader reader = new StreamReader();
		reader.setPattern("s").setStartId("1-1").
		setDuration(1000).setBatchSize(100).
		setFailurePolicy(FailurePolicy.RETRY).
		setFailureRertyInterval(5000).
		setTrimStream(false);
		new GearsBuilder(reader).
		register();
	}
}
