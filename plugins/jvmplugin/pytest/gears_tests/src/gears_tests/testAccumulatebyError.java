package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testAccumulatebyError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		accumulateBy(r->{
			return ((KeysReaderRecord)r).getStringVal();
		},(k,a,r)->{
			throw new Exception("Accumulateby Error");
		}).
		foreach(r->System.out.println(r.getClass())).
		run();
	}
}
