package gears_tests;

import java.io.IOException;
import java.util.HashMap;

import gears.GearsBuilder;
import gears.readers.StreamReader;

public class testStreamReaderFromId {
	public static void DecordStrings(HashMap<String, Object> h) {
		for(String s : h.keySet()) {
			Object val = h.get(s);
			if(val instanceof byte[]) {
				h.put(s, new String((byte[])val));
			}
			if(val instanceof HashMap) {
				DecordStrings((HashMap<String, Object>) val);
			}
		}
	}
	
	public static void main() throws IOException {
		StreamReader reader = new StreamReader();
		reader.setPattern("s").setStartId("0-0");
		new GearsBuilder(reader).
		foreach(r->{
			DecordStrings((HashMap<String, Object>)r);
		}).
		run();
	}
}
