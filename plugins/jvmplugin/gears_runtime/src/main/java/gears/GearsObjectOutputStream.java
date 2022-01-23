package gears;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Internal use
 *
 */
public class GearsObjectOutputStream extends ObjectOutputStream {

	private ByteArrayOutputStream out;
	
	public GearsObjectOutputStream(ByteArrayOutputStream out) throws IOException {
		super(out);
		this.out = out;
	}
	
	public byte[] serializeObject(Object o) throws IOException {
		this.writeObject(o);
		this.flush();
		byte[] bytes = out.toByteArray();
		out.reset();
		return bytes;
	}
	
	@Override
	protected void writeStreamHeader() {
		
	}
	
	public static GearsObjectOutputStream getGearsObjectOutputStream() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		return new GearsObjectOutputStream(out);
	}
}
