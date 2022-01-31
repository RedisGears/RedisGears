package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testAsyncRecrodForeachRaiseExcpetion {
	public static void main() {
		KeysReader reader = new KeysReader();
		GearsBuilder.CreateGearsBuilder(reader).map(r->r.getKey()).
		asyncForeach(r->{
			throw new RuntimeException("error");
		}).run();
	}
}
