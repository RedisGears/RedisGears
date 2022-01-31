package gears_tests;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;

import gears.GearsBuilder;
import gears.readers.KeysReader;

public class testNoSerializableExecutionError {
	public static void main() throws IOException {
		ServerSocket serverSocket = new ServerSocket(1111);
		KeysReader reader = new KeysReader();
		GearsBuilder gb = new GearsBuilder(reader);
		gb.map(r->{
			return (Serializable) serverSocket;
		}).run();
	}
}
