package gears_tests;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class testAccumulatebyExtractorError {
	public static void main() {
		KeysReader reader = new KeysReader();
		new GearsBuilder(reader).
		accumulateBy(r->{
			throw new Exception("AccumulatebyExtractor Error");
		},(k,a,r)->{
			if(a == null) {
				a = Integer.valueOf(0);
			}
			return (Integer)a + 1;
		}).
		foreach(r->System.out.println(r.getClass())).
		run();
	}
}
