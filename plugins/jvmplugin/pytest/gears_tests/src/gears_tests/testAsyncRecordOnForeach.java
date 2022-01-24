package gears_tests;

import gears.GearsBuilder;

import gears.GearsFuture;
import gears.readers.KeysReader;

import java.io.Serializable;

public class testAsyncRecordOnForeach {
	public static void main() {
		KeysReader reader = new KeysReader();
		GearsBuilder.CreateGearsBuilder(reader).map(r->r.getKey()).
		asyncForeach(r->{
			GearsFuture<Serializable> f = new GearsFuture<Serializable>();
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					try {
						Thread.sleep(1);
						
						f.setResult(r);
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
