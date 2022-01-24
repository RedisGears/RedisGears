package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testAccumulateby {
	
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		accumulateBy(r->{
			return ((KeysReaderRecord)r).getStringVal();
		},(k,a,r)->{
			if(a == null) {
				a = Integer.valueOf(0);
			}
			return (Integer)a + 1;
		}).
		run();
	}
	
}
