package gears;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

/**
 * Internal use
 *
 */
public class GearsByteInputStream extends InputStream {
	
	LinkedList<ByteArrayInputStream> dataList;
	
	public GearsByteInputStream() {
		this.dataList = new LinkedList<>();
	}
	
	public void addData(byte[] data) {
		ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
		this.dataList.addLast(inputStream);
	}

	@Override
	public int read() throws IOException {
		while(!dataList.isEmpty()) {
			int res = dataList.getFirst().read();
			if(res != -1) {
				return res;
			}
			dataList.removeFirst();
		}
		return -1;
	}

}
