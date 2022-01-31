package gears_tests;

import gears.GearsBuilder;
import gears.GearsFuture;
import gears.readers.KeysReader;

public class testAsyncRecrodFilterRaiseError {
	public static void main() {
		KeysReader reader = new KeysReader();
		GearsBuilder.CreateGearsBuilder(reader).map(r->r.getKey()).
		asyncFilter(r->{
			GearsFuture<Boolean> f = new GearsFuture<Boolean>();
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						Thread.sleep(1000);
						
						f.setError("error");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					
				}
			}).start();
			return f;
		}).run();
	}
}
