package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testAccumulate {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		accumulate((a, r)->{
			if(a == null) {
				a = Integer.valueOf(0);
			}
			return ((Integer)a) + 1;
		}).run();
	}
}
