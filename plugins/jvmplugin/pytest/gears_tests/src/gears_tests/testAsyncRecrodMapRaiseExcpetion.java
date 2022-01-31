package gears_tests;

import gears.GearsBuilder;
import gears.readers.ShardsIDReader;

public class testAsyncRecrodMapRaiseExcpetion {
	public static void main() {
		ShardsIDReader reader = new ShardsIDReader();
		GearsBuilder.CreateGearsBuilder(reader).
		asyncMap(r->{
			throw new RuntimeException("error");
		}).collect().count().run();
	}
}
