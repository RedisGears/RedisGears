package gears_tests;

import gears.GearsBuilder;
import gears.GearsFuture;
import gears.readers.ShardsIDReader;

public class testAsyncRecrodMapRaiseError {
	public static void main() {
		ShardsIDReader reader = new ShardsIDReader();
		GearsBuilder.CreateGearsBuilder(reader).
		asyncMap(r->{
			GearsFuture<String> f = new GearsFuture<String>();
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						Thread.sleep(1);
					
						f.setError("error");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					
				}
			}).start();
			return f;
		}).collect().count().run();
	}
}
