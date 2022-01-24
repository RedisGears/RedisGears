package gears_tests;

import java.io.IOException;
import java.util.HashMap;

import gears.GearsBuilder;
import gears.readers.StreamReader;

public class testBasicStreamRegistration {
	public static void main() throws IOException {
		StreamReader reader = new StreamReader();
		reader.setPattern("s*");
		new GearsBuilder(reader).
		map(r->{
			HashMap<String, Object> value = (HashMap<String, Object>)((HashMap<String, Object>)r).get("value");
			return "{'name': '" + new String((byte[])value.get("name")) + "'}"; 			
		}).
		repartition(r->{
			return "new_key";
		}).
		foreach(r->{
			GearsBuilder.execute("set", "new_key", r.toString());
		}).
		register();
	}
}
