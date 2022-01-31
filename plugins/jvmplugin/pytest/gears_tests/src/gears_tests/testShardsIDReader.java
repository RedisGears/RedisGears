package gears_tests;

import java.io.IOException;

import gears.GearsBuilder;
import gears.readers.ShardsIDReader;

public class testShardsIDReader {
	public static void main() throws IOException {
		ShardsIDReader reader = new ShardsIDReader();
		GearsBuilder.CreateGearsBuilder(reader).
		accumulate((a, r)->{
			if(a == null) {
				return 1;
			}
			return (Integer)a + 1;
		}).
		collect().
		accumulate((a, r)->{
			if(a == null) {
				return r;
			}
			return (Integer)a + (Integer)r;
		}).
		run();
	}
}
