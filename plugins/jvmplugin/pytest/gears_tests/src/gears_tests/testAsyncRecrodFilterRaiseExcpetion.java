package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testAsyncRecrodFilterRaiseExcpetion {
	public static void main() {
		KeysReader reader = new KeysReader();
		GearsBuilder.CreateGearsBuilder(reader).map(r->r.getKey()).
		asyncFilter(r->{
			throw new RuntimeException("error");
		}).run();
	}
}
